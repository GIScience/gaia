from dagster import MultiPartitionsDefinition, StaticPartitionsDefinition, asset, AssetIn
from pathlib import Path
import geopandas as gpd
import pandas as pd
import yaml
import os
from scripts.fetch_boundaries_hdx import download_shapefiles
from scripts.fetch_worldpop import aggregate_worldpop_to_csv
from scripts.upload_minio import upload_to_minio
from scripts.upload_to_hdx import upload_to_hdx
from scripts.fetch_floods_jrc import process_flood_impact, ALLOWED_RPS
from scripts.fetch_facilities_ohsome_overpass import fetch_ohsome, fetch_overpass
from scripts.fetch_ruralness_ghsl import compute_rural_population
from scripts.fetch_access_minio import compute_access_population
from dagster import Output
from typing import List

ASSET_CONFIG_YAML_PATH = os.path.join(os.getcwd(), "configs", "assets_config.yaml")
with open(ASSET_CONFIG_YAML_PATH) as _fp:
    _asset_config = yaml.safe_load(_fp)

COUNTRIES_YAML_PATH = os.path.join(os.getcwd(), "configs", "hdx_countries.yaml")
with open(COUNTRIES_YAML_PATH) as _fp:
    _mapping = yaml.safe_load(_fp)

ALL_COUNTRIES = list(_mapping.keys())
country_partitions = StaticPartitionsDefinition(partition_keys=ALL_COUNTRIES)


category_partitions = StaticPartitionsDefinition(
    ["demographics", "facilities", "ndvi", "crops", "rural", "access", "coping", "vulnerability", "exposure"]
)

multi_partitions = MultiPartitionsDefinition(
    {
        "country": country_partitions,
        "category": category_partitions,
    }
)


@asset(partitions_def=country_partitions)
def boundary_asset(context) -> str:
    """
    Downloads administrative boundary shapefiles from HDX and converts to GeoJSON.
    Checks which admin levels were generated and ensures the ID columns are correctly named.
    """
    country_code = context.partition_key.upper()
    download_shapefiles(country_code)

    data_dir = f"data/{country_code}"
    max_admin_level = 5  # ADM0 to ADM5
    found_cols = {}

    if os.path.exists(data_dir):
        for level in range(max_admin_level + 1):
            admin_file = os.path.join(data_dir, f"{country_code}_ADM{level}.geojson")
            expected_col = f"ADM{level}_PCODE"

            if os.path.exists(admin_file):
                gdf = gpd.read_file(admin_file)

                # If expected column exists, mark as found
                if expected_col in gdf.columns:
                    found_cols[f"ADM{level}"] = expected_col
                    continue

                # Fallback: search for a column that includes admin number and "code"
                fallback_col = None
                for col in gdf.columns:
                    col_lower = col.lower()
                    if str(level) in col_lower and ("cod" in col_lower or "id" in col_lower):
                        fallback_col = col
                        break

                if fallback_col:
                    # Rename column
                    gdf = gdf.rename(columns={fallback_col: expected_col})
                    gdf.to_file(admin_file, driver="GeoJSON")
                    context.log.info(f"{country_code}: Renamed column {fallback_col} -> {expected_col} in {admin_file}")
                    found_cols[f"ADM{level}"] = expected_col
                else:
                    context.log.warning(f"{country_code}: No suitable ID column found for ADM{level} in {admin_file}")
            else:
                context.log.warning(f"{country_code}: ADM{level} file not found ({admin_file})")

    if found_cols:
        context.log.info(f"{country_code}: Detected ID columns per admin level: {found_cols}")
    else:
        context.log.warning(f"{country_code}: No ID columns detected in any generated GeoJSON files")

    return data_dir

@asset(
    deps=["boundary_asset"],
    partitions_def=country_partitions,
)
def demographics_asset(context) -> list[str]:
    """
    For the given country (partition key), run WorldPop processing for each
    admin level specified in assets_config.yaml.
    Returns a list of output CSV paths.
    """
    country_code = context.partition_key.upper()

    # Get admin levels from config
    admin_levels = _asset_config.get("setup", {}).get("admin_levels", [])
    if not admin_levels:
        raise ValueError("No admin_levels configured in assets_config.yaml")

    outputs = []

    for admin_level in admin_levels:
        try:
            output_csv = aggregate_worldpop_to_csv(
                country_code=country_code,
                admin_level=admin_level,
                context_log=context.log
            )
            outputs.append(output_csv)
        except Exception as e:
            context.log.warning(f"Skipping {country_code} {admin_level}: {e}")
            continue
        

    return outputs

@asset(
    partitions_def=country_partitions,
    ins={"boundary_asset": AssetIn()},
)
def facilities_asset(context, boundary_asset: str) -> Output[List[str]]:
    """
    Extract health and education facilities via Ohsome or Overpass.
    """
    country_code = context.partition_key.upper()
    base_path = Path(boundary_asset if boundary_asset else f"data/{country_code}")

    admin_levels = _asset_config.get("setup", {}).get("admin_levels", [])
    if not admin_levels:
        raise ValueError("No admin_levels configured in assets_config.yaml")

    summary_paths = []

    for admin_level in admin_levels:
        boundary_path = base_path / f"{country_code}_{admin_level}.geojson"
        if not boundary_path.exists():
            context.log.warning(
                f"Skipping {country_code} {admin_level}: boundary file not found at {boundary_path}"
            )
            continue

        context.log.info(f"Processing {country_code} {admin_level} using {boundary_path}")

        output_dir = Path("data") / country_code

        api_choice = _asset_config.get("facilities_asset", {}).get("api", "").lower()

        if api_choice == "ohsome-api":
            summary_path = fetch_ohsome(
                context.log, boundary_path, output_dir, country_code, admin_level
            )
        elif api_choice == "overpass":
            summary_path = fetch_overpass(
                context.log, boundary_path, output_dir, country_code, admin_level
            )
        elif api_choice == "ohsome-parquet":
            context.log.info("Not implemented yet: ohsome-parquet")
            continue
        else:
            context.log.warning(
                f"No valid API configured for facilities_asset (got '{api_choice}')"
            )
            continue

        if summary_path:
            summary_paths.append(str(summary_path))

    if not summary_paths:
        context.log.warning(f"No facility outputs created for {country_code}")
        return Output([], metadata={"country": country_code})

    return Output(
        summary_paths,
        metadata={
            "country": country_code,
            "outputs": summary_paths,
        },
    )


@asset(
    deps=["demographics_asset", "facilities_asset"],
    partitions_def=country_partitions,
    ins={"boundary_asset": AssetIn()},
)
def exposure_flood_asset(context, boundary_asset: str) -> list[str]:
    """
    For the given country, iterate over configured admin levels and RPs.
    Generate flooded population CSVs using GLOFAS and WorldPop.
    Skips missing admin levels or boundaries.
    Returns a list of output CSV paths.
    """
    country_code = context.partition_key.upper()
    base_path = Path(boundary_asset if boundary_asset else f"data/{country_code}")

    admin_levels = _asset_config.get("setup", {}).get("admin_levels", [])
    if not admin_levels:
        raise ValueError("No admin_levels configured in assets_config.yaml")

    rps = _asset_config.get("setup", {}).get("rps", ALLOWED_RPS)
    outputs = []

    for admin_level in admin_levels:
        boundary_path = base_path / f"{country_code}_{admin_level}.geojson"
        if not boundary_path.exists():
            context.log.warning(
                f"Skipping {country_code} {admin_level}: boundary file not found at {boundary_path}"
            )
            continue

        context.log.info(f"Processing {country_code} {admin_level} using {boundary_path}")
        gdf = gpd.read_file(boundary_path)

        id_col = f"{admin_level.upper()}_PCODE"
        if id_col not in gdf.columns:
            context.log.warning(
                f"Skipping {country_code} {admin_level}: expected ID column '{id_col}' not found"
            )
            continue

        output_dir = base_path / "Output"
        os.makedirs(output_dir, exist_ok=True)

        csv_path = process_flood_impact(
            context=context.log,
            country_code=country_code,
            rps=rps,
            gdf=gdf,
            admin_level=admin_level,
            output_dir=str(output_dir),
        )
        outputs.append(csv_path)

    return outputs

@asset(
    deps=["demographics_asset"], 
    partitions_def=country_partitions,
    ins={"boundary_asset": AssetIn()},
)
def rural_asset(context, boundary_asset: str) -> list[str]:
    """
    For the given country, iterate over configured admin levels.
    Generate rural population CSVs using WorldPop + SMOD.
    Skips missing admin levels or boundaries.
    Returns a list of output CSV paths.
    """
    country_code = context.partition_key.upper()
    base_path = Path(boundary_asset if boundary_asset else f"data/{country_code}")

    admin_levels = _asset_config.get("setup", {}).get("admin_levels", [])
    if not admin_levels:
        raise ValueError("No admin_levels configured in assets_config.yaml")

    outputs = []

    for admin_level in admin_levels:
        boundary_path = base_path / f"{country_code}_{admin_level}.geojson"
        if not boundary_path.exists():
            context.log.warning(
                f"Skipping {country_code} {admin_level}: boundary file not found at {boundary_path}"
            )
            continue

        context.log.info(f"Processing {country_code} {admin_level} using {boundary_path}")
        gdf = gpd.read_file(boundary_path)

        id_col = f"{admin_level.upper()}_PCODE"
        if id_col not in gdf.columns:
            context.log.warning(
                f"Skipping {country_code} {admin_level}: expected ID column '{id_col}' not found"
            )
            continue

        output_dir = base_path / "Output"
        os.makedirs(output_dir, exist_ok=True)

        csv_path = compute_rural_population(
            country_code=country_code,
            admin_level=admin_level,
            gdf=gdf,
            work_dir=str(Path()),
            output_dir=str(output_dir),
            context=context.log, 
        )
        outputs.append(csv_path)

    return outputs


@asset(
    deps=["demographics_asset"], 
    partitions_def=country_partitions,
    ins={"boundary_asset": AssetIn()},
)
def access_asset(context, boundary_asset: str) -> list[str]:
    """
    For the given country, iterate over configured admin levels.
    Generate accessibility exposure CSVs using HEiGIT isochrones + WorldPop.
    Skips missing admin levels or boundaries.
    Returns a list of output CSV paths.
    """
    country_code = context.partition_key.upper()
    base_path = Path(boundary_asset if boundary_asset else f"data/{country_code}")

    admin_levels = _asset_config.get("setup", {}).get("admin_levels", [])
    if not admin_levels:
        raise ValueError("No admin_levels configured in assets_config.yaml")

    outputs = []

    for admin_level in admin_levels:
        boundary_path = base_path / f"{country_code}_{admin_level}.geojson"
        if not boundary_path.exists():
            context.log.warning(
                f"Skipping {country_code} {admin_level}: boundary file not found at {boundary_path}"
            )
            continue

        context.log.info(f"Processing {country_code} {admin_level} using {boundary_path}")
        gdf = gpd.read_file(boundary_path)

        id_col = f"{admin_level.upper()}_PCODE"
        if id_col not in gdf.columns:
            context.log.warning(
                f"Skipping {country_code} {admin_level}: expected ID column '{id_col}' not found"
            )
            continue

        output_dir = base_path / "Output"
        os.makedirs(output_dir, exist_ok=True)

        csv_path = compute_access_population(
            country_code=country_code,
            admin_level=admin_level,
            gdf_admin=gdf,
            work_dir=base_path / "Temporary",
            output_dir=output_dir,
            context=context.log,
        )

        outputs.append(csv_path)

    return outputs

@asset(
    deps=["access_asset", "facilities_asset"],
    partitions_def=country_partitions,
)
def coping_asset(context, access_asset: List[str], facilities_asset: List[str]) -> list[str]:
    """
    Combine accessibility and facilities CSVs into a single coping dataset.
    Joins on the ADM*_PCODE column per admin level.
    Produces one coping CSV per admin level in Output/.
    """
    country_code = context.partition_key.upper()
    outputs = []

    for access_csv, facilities_csv in zip(access_asset, facilities_asset):
        if not os.path.exists(access_csv) or not os.path.exists(facilities_csv):
            context.log.warning(
                f"Skipping merge for {country_code}: missing files {access_csv}, {facilities_csv}"
            )
            continue

        try:
            df_access = pd.read_csv(access_csv)
            df_facilities = pd.read_csv(facilities_csv)

            # detect admin code column automatically (ADM0_PCODE, ADM1_PCODE, etc.)
            id_col = [c for c in df_access.columns if c.endswith("_PCODE")]
            if not id_col:
                context.log.warning(f"Skipping {access_csv}: no *_PCODE column found")
                continue
            id_col = id_col[0]

            merged = pd.merge(df_access, df_facilities, on=id_col, how="left")

            # --- keep only one ADM_PCODE column ---
            adm_cols = [c for c in merged.columns if c.startswith("ADM") and c.endswith("_PCODE")]
            if "ADM_PCODE_x" in merged.columns or "ADM_PCODE_y" in merged.columns:
                merged["ADM_PCODE"] = merged["ADM_PCODE_x"].combine_first(merged["ADM_PCODE_y"])
                merged.drop(columns=[c for c in ["ADM_PCODE_x", "ADM_PCODE_y"] if c in merged.columns], inplace=True)
            elif "ADM_PCODE" in merged.columns and adm_cols.count("ADM_PCODE") > 1:
                # In case of duplicate ADM_PCODE columns from merge quirks
                merged = merged.loc[:, ~merged.columns.duplicated()]

            admin_level = id_col.split("_")[0]  # e.g., ADM2
            output_dir = Path("data") / country_code / "Output"
            output_dir.mkdir(parents=True, exist_ok=True)

            output_path = output_dir / f"{country_code}_{admin_level}_coping.csv"
            merged.to_csv(output_path, index=False)
            outputs.append(str(output_path))

            context.log.info(f"[{country_code}] Wrote coping CSV: {output_path}")

        except Exception as e:
            context.log.warning(f"Failed to merge {access_csv} and {facilities_csv}: {e}")

    if not outputs:
        context.log.warning(f"No coping outputs created for {country_code}")
    return outputs

@asset(
    deps=["demographics_asset", "rural_asset"],
    partitions_def=country_partitions,
)
def vulnerability_asset(context, demographics_asset: List[str], rural_asset: List[str]) -> list[str]:
    """
    Combine demographics and rural population CSVs into a single vulnerability dataset.
    Joins on the ADM*_PCODE column per admin level.
    Produces one vulnerability CSV per admin level in Output/.
    """
    country_code = context.partition_key.upper()
    outputs = []

    for demo_csv, rural_csv in zip(demographics_asset, rural_asset):
        if not os.path.exists(demo_csv) or not os.path.exists(rural_csv):
            context.log.warning(f"Skipping merge for {country_code}: missing files {demo_csv}, {rural_csv}")
            continue

        try:
            df_demo = pd.read_csv(demo_csv)
            df_rural = pd.read_csv(rural_csv)

            # detect admin code column automatically (ADM0_PCODE, ADM1_PCODE, etc.)
            id_col = [c for c in df_demo.columns if c.endswith("_PCODE")]
            if not id_col:
                context.log.warning(f"Skipping {demo_csv}: no *_PCODE column found")
                continue
            id_col = id_col[0]

            merged = pd.merge(df_demo, df_rural, on=id_col, how="left")

            # keep only one ADM_PCODE column
            adm_cols = [c for c in merged.columns if c.startswith("ADM") and c.endswith("_PCODE")]
            if "ADM_PCODE_x" in merged.columns or "ADM_PCODE_y" in merged.columns:
                merged["ADM_PCODE"] = merged["ADM_PCODE_x"].combine_first(merged["ADM_PCODE_y"])
                merged.drop(columns=[c for c in ["ADM_PCODE_x", "ADM_PCODE_y"] if c in merged.columns], inplace=True)
            elif "ADM_PCODE" in merged.columns and adm_cols.count("ADM_PCODE") > 1:
                # In case of duplicate ADM_PCODE columns from merge quirks
                merged = merged.loc[:, ~merged.columns.duplicated()]

            admin_level = id_col.split("_")[0]  # e.g., ADM2
            output_dir = Path("data") / country_code / "Output"
            output_dir.mkdir(parents=True, exist_ok=True)

            output_path = output_dir / f"{country_code}_{admin_level}_vulnerability.csv"
            merged.to_csv(output_path, index=False)
            outputs.append(str(output_path))

            context.log.info(f"[{country_code}] Wrote vulnerability CSV: {output_path}")

        except Exception as e:
            context.log.warning(f"Failed to merge {demo_csv} and {rural_csv}: {e}")

    if not outputs:
        context.log.warning(f"No vulnerability outputs created for {country_code}")
    return outputs

@asset(
    deps=["demographics_asset", "facilities_asset", "coping_asset", "exposure_flood_asset", "vulnerability_asset"],
    partitions_def=multi_partitions,
)
def upload_minio_asset(context):
    parts = context.partition_key.split("|")
    country, category = parts[1], parts[0]
    
    output_dir = os.path.join("data", country, "Output")

    if not os.path.isdir(output_dir):
        raise FileNotFoundError(f"[{country}] Output folder not found: {output_dir}")

    files = os.listdir(output_dir)
    matched = [f for f in files if category in f.lower()]

    if not matched:
        context.log.info(f"[{country}] No '{category}' outputs found in {output_dir}")
        return

    context.log.info(f"[{country}] Found {category} outputs: {matched}")
    upload_to_minio(country, category)
    context.log.info(f"[{country}] Uploaded {category} dataset(s) to MinIO successfully.")

@asset(
    deps=["upload_minio_asset"], 
    partitions_def=country_partitions,
)
def upload_hdx_asset(context):
    country = context.partition_key.upper()

    hdx_config_path = _asset_config["hdx_asset"].get("config_path", "")
    countries_config = _asset_config["hdx_asset"].get("countries_config", "configs/hdx_countries.yaml")

    context.log.info(f"[{country}] Uploading dataset to HDX")

    url = upload_to_hdx(country, hdx_config_path, countries_config)

    context.log.info(f"[{country}] Upload to HDX complete: {url}")
    return url

