from dagster import MultiPartitionsDefinition, StaticPartitionsDefinition, asset, Output, AssetIn
from pathlib import Path
import geopandas as gpd
import pandas as pd
import yaml
import os
import shutil
from scripts.fetch_boundaries_hdx import download_shapefiles
from scripts.fetch_worldpop import aggregate_worldpop_to_csv
from scripts.upload_minio import upload_to_minio
from scripts.upload_to_hdx import upload_to_hdx
from scripts.fetch_floods_jrc import process_flood_impact, ALLOWED_RPS
from scripts.fetch_facilities_ohsome_overpass import fetch_ohsome, fetch_overpass
from scripts.fetch_ruralness_ghsl import compute_rural_population
from scripts.fetch_access_minio import compute_access_population
from scripts.fetch_cyclones_ncei import calculate_cyclone_exposure
from typing import List
import numpy as np
import requests
import subprocess
import tempfile

LAYER_PREFIXES = ["cop", "exp", "vul"]

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

def find_best_available_admin_level(base_path: Path, country_code: str, admin_level: str):
    """
    Given the requested admin level (e.g. 'ADM2'), fallback to ADM1 → ADM0 when files are missing.
    Returns (final_level, path) or (None, None) if nothing exists.
    """
    lvl_num = int(admin_level.replace("ADM", ""))

    for test_lvl in range(lvl_num, -1, -1):  # e.g. 2 → 1 → 0
        level_name = f"ADM{test_lvl}"
        boundary_path = base_path / f"{country_code}_{level_name}.geojson"
        if boundary_path.exists():
            return level_name, boundary_path

    return None, None


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
        orig_level = admin_level

        # Extract numeric (ADM2 → 2)
        lvl_num = int(admin_level.replace("ADM", ""))

        # Try ADM(level), ADM(level-1), ... ADM0
        fallback_levels = [f"ADM{n}" for n in range(lvl_num, -1, -1)]

        success = False

        for test_level in fallback_levels:
            try:
                output_csv = aggregate_worldpop_to_csv(
                    country_code=country_code,
                    admin_level=test_level,
                    context_log=context.log
                )

                if test_level != orig_level:
                    context.log.info(
                        f"[{country_code}] WorldPop fallback: using {test_level} "
                        f"instead of requested {orig_level}"
                    )

                outputs.append(output_csv)
                success = True
                break  # exit fallback loop

            except Exception as e:
                context.log.warning(
                    f"[{country_code}] WorldPop failed for {test_level}: {e}"
                )
                continue

        if not success:
            context.log.warning(
                f"[{country_code}] No available admin level found for requested {orig_level} "
                f"(tried {fallback_levels})"
            )
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
        orig_level = admin_level
        level, boundary_path = find_best_available_admin_level(base_path, country_code, admin_level)

        if not level:
            context.log.warning(f"Skipping {country_code}: no boundary found for {orig_level} or lower levels")
            continue

        if level != orig_level:
            context.log.info(f"[{country_code}] Using fallback admin level {level} (requested {orig_level})")

        admin_level = level  # use the found level

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
        raise RuntimeError(f"No facility outputs created for {country_code}")

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
        orig_level = admin_level
        level, boundary_path = find_best_available_admin_level(base_path, country_code, admin_level)

        if not level:
            context.log.warning(f"Skipping {country_code}: no boundary found for {orig_level} or lower levels")
            continue

        if level != orig_level:
            context.log.info(f"[{country_code}] Using fallback admin level {level} (requested {orig_level})")

        admin_level = level  # use the found level

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
    deps=["demographics_asset", "facilities_asset"],
    partitions_def=country_partitions,
    ins={"boundary_asset": AssetIn()},
)
def exposure_cyclone_asset(context, boundary_asset: str) -> list[str]:
    """
    Generate cyclone exposure CSVs using IBTrACS data and WorldPop/facilities data.
    For each configured admin level, computes exposed populations and facilities.
    """
    country_code = context.partition_key.upper()
    base_path = Path(boundary_asset if boundary_asset else f"data/{country_code}")

    admin_levels = _asset_config.get("setup", {}).get("admin_levels", [])
    if not admin_levels:
        raise ValueError("No admin_levels configured in assets_config.yaml")

    outputs = []

    for admin_level in admin_levels:
        orig_level = admin_level
        level, boundary_path = find_best_available_admin_level(base_path, country_code, admin_level)

        if not level:
            context.log.warning(f"Skipping {country_code}: no boundary found for {orig_level} or lower levels")
            continue

        if level != orig_level:
            context.log.info(f"[{country_code}] Using fallback admin level {level} (requested {orig_level})")

        admin_level = level  # use the found level

        context.log.info(f"Processing {country_code} {admin_level} using {boundary_path}")
        gdf = gpd.read_file(boundary_path)

        id_col = f"{admin_level.upper()}_PCODE"
        if id_col not in gdf.columns:
            context.log.warning(
                f"Skipping {country_code} {admin_level}: expected ID column '{id_col}' not found"
            )
            continue

        try:
            csv_path = calculate_cyclone_exposure(
                context=context.log,
                country_code=country_code,
                admin_level=admin_level,
            )
            if csv_path:
                outputs.append(csv_path)
            else:
                context.log.warning(f"No output produced for {country_code} {admin_level}")
        except Exception as e:
            context.log.error(f"Error processing {country_code} {admin_level}: {e}")

    if not outputs:
        context.log.warning(f"No cyclone exposure outputs generated for {country_code}")
    else:
        context.log.info(f"Generated {len(outputs)} cyclone exposure CSVs for {country_code}")

    return outputs


@asset(
    partitions_def=country_partitions,
    ins={"boundary_asset": AssetIn(),
        "demographics_asset": AssetIn(),
},
)
def rural_asset(context, boundary_asset: str, demographics_asset) -> list[str]:
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
        orig_level = admin_level
        level, boundary_path = find_best_available_admin_level(base_path, country_code, admin_level)

        if not level:
            context.log.warning(f"Skipping {country_code}: no boundary found for {orig_level} or lower levels")
            continue

        if level != orig_level:
            context.log.info(f"[{country_code}] Using fallback admin level {level} (requested {orig_level})")

        admin_level = level  # use the found level

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
        orig_level = admin_level
        level, boundary_path = find_best_available_admin_level(base_path, country_code, admin_level)

        if not level:
            context.log.warning(f"Skipping {country_code}: no boundary found for {orig_level} or lower levels")
            continue

        if level != orig_level:
            context.log.warning(f"[{country_code}] Using fallback admin level {level} (requested {orig_level})")

        admin_level = level  # use the found level

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
    deps=["boundary_asset", "upload_minio_asset"],
    partitions_def=country_partitions,
)
def prep_visualization_asset(context) -> list[str]:

    country_code = context.partition_key.upper()
    output_dir = Path("data") / country_code / "Output"
    output_dir.mkdir(parents=True, exist_ok=True)

    PREFIX_MAP = {
        "coping": "cop_",
        "vulnerability": "vul_",
        "flood_exposure": "exp_flo_",
        "cyclone_exposure": "exp_cyc_",
    }

    REMOTE_FILES = {
        "coping": "{country}_{adm}_coping.csv",
        "vulnerability": "{country}_{adm}_vulnerability.csv",
        "flood_exposure": "{country}_{adm}_flood_exposure.csv",
        "cyclone_exposure": "{country}_{adm}_cyclone_exposure.csv",
    }

    OPTIONAL_SOURCES = {"flood_exposure", "cyclone_exposure"}

    BASE_URL = "https://hot.storage.heigit.org/heigit-hdx-public/risk_assessment_inputs/{country}/{file}"

    # store dataframes grouped by admin level
    source_map: dict[str, dict[str, pd.DataFrame]] = {}

    # ------------------------------------------------------------------
    # Step 1 – Load remote CSVs with ADM fallback
    # ------------------------------------------------------------------
    for source_name, filename_template in REMOTE_FILES.items():

        df = None
        adm = None

        for candidate_adm in ["ADM2", "ADM1"]:

            filename = filename_template.format(
                country=country_code,
                adm=candidate_adm,
            )
            url = BASE_URL.format(
                country=country_code.lower(),   # folder
                file=filename                   # filename stays uppercase
            )
            try:
                context.log.info(
                    f"[{country_code}] Trying {source_name} ({candidate_adm}): {url}"
                )
                df = pd.read_csv(url)
                adm = candidate_adm
                break

            except Exception as e:
                context.log.warning(
                    f"[{country_code}] Could not load {source_name} for {candidate_adm}: {e}"
                )

        if df is None:

            if source_name in OPTIONAL_SOURCES:
                context.log.warning(
                    f"[{country_code}] Optional exposure file '{source_name}' not found. Skipping."
                )
                continue

            context.log.error(
                f"[{country_code}] Missing required input file for '{source_name}'. "
                "Tried ADM2 and ADM1."
            )
            raise RuntimeError(
                f"[{country_code}] Required input '{source_name}' not found."
            )

        prefix = PREFIX_MAP[source_name]

        rename_cols = {
            c: f"{prefix}{c}"
            for c in df.columns
            if not c.upper().startswith("ADM")
        }

        df = df.rename(columns=rename_cols)

        source_map.setdefault(adm, {})[source_name] = df

        context.log.info(
            f"[{country_code}] Loaded {source_name} for {adm}"
        )

    if not source_map:
        context.log.warning(f"[{country_code}] No input data found; skipping combined Parquet.")
        return []

    output_paths = []

    # ------------------------------------------------------------------
    # Step 2 – Merge datasets
    # ------------------------------------------------------------------
    for adm, sources in source_map.items():

        id_col = f"{adm}_PCODE"
        merged: pd.DataFrame | None = None

        for source_name, df in sources.items():

            if id_col not in df.columns:

                if "ADM_PCODE" in df.columns:
                    df = df.rename(columns={"ADM_PCODE": id_col})

                else:
                    context.log.warning(
                        f"[{country_code}] {source_name} has no '{id_col}' column – skipping"
                    )
                    continue

            if "ADM_PCODE" in df.columns and id_col != "ADM_PCODE":
                df = df.drop(columns=["ADM_PCODE"])

            if merged is None:
                merged = df

            else:

                overlap = [c for c in df.columns if c in merged.columns and c != id_col]

                if overlap:
                    context.log.warning(
                        f"[{country_code}] Duplicate columns from {source_name}: {overlap}"
                    )

                merged = pd.merge(
                    merged,
                    df,
                    on=id_col,
                    how="outer",
                    suffixes=("", f"_{source_name}"),
                )

        if merged is None or merged.empty:
            context.log.warning(f"[{country_code}] Nothing to write for {adm}.")
            continue

        out_path = output_dir / f"{country_code}_{adm}_combined.parquet"

        merged.to_parquet(out_path, index=False, engine="pyarrow")

        output_paths.append(str(out_path))

        context.log.info(
            f"[{country_code}] Written combined Parquet ({len(merged)} rows, "
            f"{len(merged.columns)} cols): {out_path}"
        )

    # ------------------------------------------------------------------
    # Step 3 – Generate PMTiles
    # ------------------------------------------------------------------
    adm2_geojson = Path("data") / country_code / f"{country_code}_ADM2.geojson"
    adm1_geojson = Path("data") / country_code / f"{country_code}_ADM1.geojson"

    # Determine which boundary to use
    if adm2_geojson.exists():
        boundary_geojson = adm2_geojson
        level = "ADM2"
    elif adm1_geojson.exists():
        boundary_geojson = adm1_geojson
        level = "ADM1"
        context.log.warning(
            f"[{country_code}] ADM2 boundary not found. Falling back to ADM1."
        )
    else:
        context.log.warning(
            f"[{country_code}] No ADM1 or ADM2 boundary found; skipping PMTiles."
        )
        return output_paths

    context.log.info(f"[{country_code}] Generating PMTiles from {boundary_geojson}")

    gdf = gpd.read_file(boundary_geojson)

    pcode_field = f"{level}_PCODE"

    # If expected PCODE column doesn't exist, try to detect it
    if pcode_field not in gdf.columns:

        candidate = next(
            (
                c
                for c in gdf.columns
                if level[-1] in c and ("pcode" in c.lower() or "cod" in c.lower())
            ),
            None,
        )

        if candidate:
            gdf = gdf.rename(columns={candidate: pcode_field})
            context.log.info(f"[{country_code}] Renamed '{candidate}' → '{pcode_field}'")

        else:
            context.log.warning(
                f"[{country_code}] No {pcode_field}-like column found; skipping PMTiles."
            )
            return output_paths

    # Standardize schema
    gdf["ADM_PCODE"] = gdf[pcode_field]

    gdf = gdf[[pcode_field, "ADM_PCODE", "geometry"]]

    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    pmtiles_path = output_dir / f"{country_code}_{level}.pmtiles"

    with tempfile.NamedTemporaryFile(suffix=".geojson", delete=False, mode="w") as tmp:
        tmp_path = tmp.name
        gdf.to_file(tmp_path, driver="GeoJSON")

    try:
        result = subprocess.run(
            [
                "tippecanoe",
                "--output", str(pmtiles_path),
                "--layer", "boundary",
                "--minimum-zoom", "0",
                "--maximum-zoom", "10",
                "--force",
                tmp_path,
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            context.log.error(f"[{country_code}] tippecanoe failed:\n{result.stderr}")

        else:
            output_paths.append(str(pmtiles_path))
            context.log.info(f"[{country_code}] PMTiles written: {pmtiles_path}")

    finally:
        os.unlink(tmp_path)

    return output_paths


def normalize_indicators(indicators_df):
    def normalize(x):
        range_min = x.min()
        range_max = x.max()
        if pd.isna(range_min) or pd.isna(range_max) or range_max == range_min:
            return x
        return (x - range_min) / (range_max - range_min)

    for prefix in LAYER_PREFIXES:
        cols = [c for c in indicators_df.columns if c.startswith(prefix + "_")]
        indicators_df[cols] = indicators_df[cols].apply(normalize, axis=0)

    return indicators_df


def guess_missing_indicators(df):
    coping_columns = [c for c in df.columns if c.startswith("cop_")]
    df[coping_columns] = df[coping_columns].fillna(0)

    vulnerability_columns = [c for c in df.columns if c.startswith("vul_")]
    df[vulnerability_columns] = df[vulnerability_columns].fillna(1)

    return df


def calculate_geometric_mean(col1, col2):
    return np.sqrt(col1 * col2)

@asset(
    deps=["prep_visualization_asset"],
    partitions_def=country_partitions,
)
def risk_score_asset(context, prep_visualization_asset: list[str]) -> list[str]:

    country_code = context.partition_key.upper()
    output_paths = []

    for parquet_path in prep_visualization_asset:

        if not parquet_path.endswith("_combined.parquet"):
            continue

        context.log.info(f"[{country_code}] Processing {parquet_path}")

        df = pd.read_parquet(parquet_path)

        # ------------------------------------------------
        # Identify ID column
        # ------------------------------------------------
        id_col = "ADM2_PCODE" if "ADM2_PCODE" in df.columns else [
            c for c in df.columns if c.endswith("_PCODE")
        ][0]

        df = df.set_index(id_col)

        # ------------------------------------------------
        # Identify indicator groups
        # ------------------------------------------------
        coping_cols = [c for c in df.columns if c.startswith("cop_")]
        vulnerability_cols = [c for c in df.columns if c.startswith("vul_")]
        flood_cols = [c for c in df.columns if c.startswith("exp_flo_")]
        cyclone_cols = [c for c in df.columns if c.startswith("exp_cyc_")]

        # ------------------------------------------------
        # Build indicator dataframe (Raw features)
        # ------------------------------------------------
        coping = df[coping_cols]
        vulnerability = df[vulnerability_cols]

        exposures = {}
        # We'll keep a list of all raw exposure columns to include later
        raw_exposure_cols = [] 

        if flood_cols:
            flood = df[flood_cols].copy()
            raw_exposure_cols.extend(flood_cols)
            flood.columns = [c.replace("exp_flo_", "exp_") for c in flood.columns]
            exposures["flood"] = flood

        if cyclone_cols:
            cyclone = df[cyclone_cols].copy()
            raw_exposure_cols.extend(cyclone_cols)
            cyclone.columns = [c.replace("exp_cyc_", "exp_") for c in cyclone.columns]
            exposures["cyclone"] = cyclone

        # This contains your columns BEFORE normalization
        indicators = pd.concat(
            [coping, vulnerability] + list(exposures.values()),
            axis=1
        )
        
        # Create a copy of the RAW columns to merge later
        # We use the original prefix names from 'df' to keep them distinct
        raw_features = df[coping_cols + vulnerability_cols + raw_exposure_cols]

        # ------------------------------------------------
        # Normalize indicators
        # ------------------------------------------------
        normalized = normalize_indicators(indicators.copy())
        full = guess_missing_indicators(normalized)

        # ------------------------------------------------
        # Compute shared components
        # ------------------------------------------------
        cop_vals = full[[c for c in full.columns if c.startswith("cop_")]]
        vul_vals = full[[c for c in full.columns if c.startswith("vul_")]]

        cop_score = (1 - cop_vals).mean(axis=1)
        vul_score = vul_vals.mean(axis=1)

        sus_score = calculate_geometric_mean(vul_score, cop_score)

        # ------------------------------------------------
        # Prepare result dataframe
        # ------------------------------------------------
        results = pd.DataFrame(index=full.index)
        
        # Join the raw features back in here
        results = results.join(raw_features)

        results["cop"] = cop_score
        results["vul"] = vul_score
        
        # ------------------------------------------------
        # Loop over exposure types
        # ------------------------------------------------
        for exp_type, exp_df in exposures.items():

            exp_cols = [c.replace("exp_flo_", "exp_").replace("exp_cyc_", "exp_") for c in exp_df.columns]
            exp_vals = full[exp_cols]

            exp_score = exp_vals.mean(axis=1)

            risk = calculate_geometric_mean(exp_score, sus_score)

            results[f"exp_{exp_type}"] = exp_score
            results[f"sus_{exp_type}"] = sus_score
            results[f"risk_{exp_type}"] = risk
            results[f"ranking_{exp_type}"] = risk.rank(ascending=False)

        results.reset_index(inplace=True)

        # ------------------------------------------------
        # Save output
        # ------------------------------------------------
        out_path = Path(parquet_path).with_name(
            Path(parquet_path).stem.replace("_combined", "_risk") + ".parquet"
        )

        results.to_parquet(out_path, index=False)

        context.log.info(f"[{country_code}] Risk scores written: {out_path}")

        output_paths.append(str(out_path))

    return output_paths

@asset(
    deps=["prep_visualization_asset", "risk_score_asset"],
    partitions_def=country_partitions,
)
def upload_viz_minio_asset(context):

    country = context.partition_key.upper()
    output_dir = os.path.join("data", country, "Output")

    if not os.path.isdir(output_dir):
        raise FileNotFoundError(f"[{country}] Output folder not found: {output_dir}")

    files = os.listdir(output_dir)

    pmtiles_files = [f for f in files if f.endswith(".pmtiles")]
    risk_files = [f for f in files if "_risk.parquet" in f]

    if not pmtiles_files and not risk_files:
        context.log.warning(f"[{country}] No visualization outputs found.")
        return

    context.log.info(f"[{country}] Found visualization outputs: {pmtiles_files + risk_files}")

    # This will upload to bucket/{country}/visualization/
    if pmtiles_files:
        upload_to_minio(country, "pmtiles")

    if risk_files:
        upload_to_minio(country, "risk")

    context.log.info(f"[{country}] Visualization datasets uploaded to MinIO successfully.")

     
@asset(
    deps=["demographics_asset", "facilities_asset", "coping_asset", "exposure_flood_asset", "exposure_cyclone_asset", "vulnerability_asset"],
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

    url = upload_to_hdx(country, hdx_config_path, countries_config, context.log)

    context.log.info(f"[{country}] Upload to HDX complete: {url}")
    return url

    
@asset(
    deps=["upload_hdx_asset"], 
    partitions_def=country_partitions,
)
def check_hdx_downloads_asset(context):
    """
    Check that uploaded datasets are accessible on HDX (HOT storage public links).

    Rules:
    - If all expected files exist → success
    - If no files exist → success (country not on HDX)
    - If some files exist but at least one is missing → fail
    """

    country = context.partition_key.upper()

    FILE_TYPES = [
        "access",
        "coping",
        "demographics",
        "facilities",
        "flood_exposure",
        "rural_population",
        "vulnerability",
    ]

    ADM_LEVELS = ["ADM2", "ADM1"]

    BASE_HDX_URL = (
        "https://hot.storage.heigit.org/heigit-hdx-public/"
        "risk_assessment_inputs/{country}/{filename}"
    )

    missing_files = []
    existing_files = []

    for file_type in FILE_TYPES:
        file_found = False
        for adm in ADM_LEVELS:
            filename = f"{country}_{adm}_{file_type}.csv"
            url = BASE_HDX_URL.format(country=country.lower(), filename=filename)

            try:
                r = requests.head(url, timeout=30)
                if r.status_code == 200:
                    context.log.info(f"[{country}] HDX file accessible: {filename}")
                    existing_files.append(filename)
                    file_found = True
                    break  # stop at first available ADM level
                elif r.status_code == 404:
                    continue  # try next ADM level
                else:
                    context.log.warning(f"[{country}] HDX file returned {r.status_code}: {filename}")
                    missing_files.append((filename, f"HTTP {r.status_code}"))
                    file_found = True
                    break
            except Exception as e:
                context.log.error(f"[{country}] Error accessing HDX file {filename}: {e}")
                missing_files.append((filename, str(e)))
                file_found = True
                break

        if not file_found:
            context.log.warning(f"[{country}] HDX file not found: {file_type} (tried ADM2 and ADM1)")
            missing_files.append((f"{country}_ADM2_or_ADM1_{file_type}.csv", "missing"))

    if 0 < len(existing_files) < len(FILE_TYPES):
        # Some files exist but not all → fail
        error_msg = "\n".join([f"{fname}: {reason}" for fname, reason in missing_files])
        raise RuntimeError(f"[{country}] Some HDX files are missing or not accessible:\n{error_msg}")

    # Otherwise:
    # - All files exist → success
    # - No files exist → success (country not on HDX)
    if len(existing_files) == 0:
        context.log.info(f"[{country}] No HDX files found, assuming country not uploaded → OK")
    else:
        context.log.info(f"[{country}] All HDX files are accessible")

    return True


@asset(
    deps=[
        "boundary_asset",
        "demographics_asset",
        "facilities_asset",
        "exposure_flood_asset",
        "exposure_cyclone_asset",
        "rural_asset",
        "access_asset",
        "coping_asset",
        "vulnerability_asset",
    ],
    partitions_def=country_partitions,
)
def cleanup_asset(context):
    """
    Deletes ALL intermediate files for the given country,
    keeping ONLY the Output/ folder.

    Structure kept:
        data/<COUNTRY>/
            Output/
                final CSVs ...

    Everything else is deleted:
        Temporary/
        *.tif, *.gpkg, *.zip
        intermediate *_pop.csv
        ohsome/parquet temp files
        isochrone caching
        any other intermediate folders
    """
    country_code = context.partition_key.upper()
    base_dir = Path("data") / country_code

    if not base_dir.exists():
        context.log.warning(f"[{country_code}] No data directory to clean.")
        return

    output_dir = base_dir / "Output"

    # --------------------------------------------------------
    # Step 1: Collect what should be preserved
    # --------------------------------------------------------
    preserved = set()

    # Always preserve Output/
    if output_dir.exists():
        preserved.add(output_dir.resolve())

        # preserve contents of Output/
        for p in output_dir.rglob("*"):
            preserved.add(p.resolve())

    # --------------------------------------------------------
    # Step 2: Delete all other files/folders in data/<COUNTRY>
    # --------------------------------------------------------
    for item in base_dir.iterdir():
        item_resolved = item.resolve()

        if item_resolved in preserved:
            continue  # skip Output/

        if item.is_dir():
            # delete folder except Output
            try:
                shutil.rmtree(item)
                context.log.info(f"[{country_code}] Removed folder: {item}")
            except Exception as e:
                context.log.warning(f"[{country_code}] Failed removing {item}: {e}")
        else:
            # delete file
            try:
                item.unlink()
                context.log.info(f"[{country_code}] Removed file: {item}")
            except Exception as e:
                context.log.warning(f"[{country_code}] Failed removing {item}: {e}")

    context.log.info(f"[{country_code}] Cleanup complete.")