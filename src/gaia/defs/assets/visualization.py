import os
import subprocess
import tempfile
from pathlib import Path
from typing import List

import geopandas as gpd
import pandas as pd
import dagster as dg

from gaia.defs.partitions import country_partitions
from gaia.defs.resources import S3Resource
from gaia.defs.utils import (
    normalize_indicators,
    guess_missing_indicators,
    calculate_geometric_mean,
)


@dg.asset(
    deps=["boundary_asset", "upload_s3_asset"],
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
        "drought_exposure": "exp_dro_",
    }

    REMOTE_FILES = {
        "coping": "{country}_{adm}_coping.csv",
        "vulnerability": "{country}_{adm}_vulnerability.csv",
        "flood_exposure": "{country}_{adm}_flood_exposure.csv",
        "cyclone_exposure": "{country}_{adm}_cyclone_exposure.csv",
        "drought_exposure": "{country}_{adm}_drought_exposure.csv",
    }

    OPTIONAL_SOURCES = {"flood_exposure", "cyclone_exposure", "drought_exposure"}

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
                country=country_code.lower(),  # folder
                file=filename,  # filename stays uppercase
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
            c: f"{prefix}{c}" for c in df.columns if not c.upper().startswith("ADM")
        }

        df = df.rename(columns=rename_cols)

        source_map.setdefault(adm, {})[source_name] = df

        context.log.info(f"[{country_code}] Loaded {source_name} for {adm}")

    if not source_map:
        context.log.warning(
            f"[{country_code}] No input data found; skipping combined Parquet."
        )
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
            context.log.info(
                f"[{country_code}] Renamed '{candidate}' → '{pcode_field}'"
            )
        else:
            context.log.warning(
                f"[{country_code}] No {pcode_field}-like column found; skipping PMTiles."
            )
            return output_paths

    # Standardize schema
    gdf["ADM_PCODE"] = gdf[pcode_field]

    # Detect the name column (e.g. ADM2_EN, ADM2_NAME, adm2_en, adm2_name)
    level_num = level[-1]
    name_col = next(
        (
            c
            for c in gdf.columns
            if level_num in c and ("en" in c.lower() or "name" in c.lower())
        ),
        None,
    )

    name_field = f"{level}_NAME"
    keep_cols = [pcode_field, "ADM_PCODE"]
    if name_col:
        gdf[name_field] = gdf[name_col]
        keep_cols.append(name_field)
        context.log.info(f"[{country_code}] Using '{name_col}' as {name_field}")

    keep_cols.append("geometry")
    gdf = gdf[keep_cols]

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
                "--output",
                str(pmtiles_path),
                "--layer",
                "boundary",
                "--minimum-zoom",
                "0",
                "--maximum-zoom",
                "10",
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


@dg.asset(
    deps=["prep_visualization_asset"],
    partitions_def=country_partitions,
)
def risk_score_asset(context, prep_visualization_asset: List[str]) -> list[str]:
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
        id_col = (
            "ADM2_PCODE"
            if "ADM2_PCODE" in df.columns
            else [c for c in df.columns if c.endswith("_PCODE")][0]
        )

        df = df.set_index(id_col)

        # ------------------------------------------------
        # Identify indicator groups
        # ------------------------------------------------
        coping_cols = [c for c in df.columns if c.startswith("cop_")]
        vulnerability_cols = [c for c in df.columns if c.startswith("vul_")]
        flood_cols = [c for c in df.columns if c.startswith("exp_flo_")]
        cyclone_cols = [c for c in df.columns if c.startswith("exp_cyc_")]
        drought_cols = [c for c in df.columns if c.startswith("exp_dro_")]

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

        if drought_cols:
            drought = df[drought_cols].copy()
            raw_exposure_cols.extend(drought_cols)
            drought.columns = [c.replace("exp_dro_", "exp_") for c in drought.columns]
            exposures["drought"] = drought

        # This contains your columns BEFORE normalization
        indicators = pd.concat(
            [coping, vulnerability] + list(exposures.values()), axis=1
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
            exp_cols = [
                c.replace("exp_flo_", "exp_")
                .replace("exp_cyc_", "exp_")
                .replace("exp_dro_", "exp_")
                for c in exp_df.columns
            ]
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


@dg.asset(
    deps=["prep_visualization_asset", "risk_score_asset"],
    partitions_def=country_partitions,
)
def upload_viz_s3_asset(context, s3: S3Resource) -> None:
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

    context.log.info(
        f"[{country}] Found visualization outputs: {pmtiles_files + risk_files}"
    )

    # This will upload to bucket/{country}/visualization/
    if pmtiles_files:
        s3.upload(country, "pmtiles")

    if risk_files:
        s3.upload(country, "risk")

    context.log.info(f"[{country}] Visualization datasets uploaded to S3 successfully.")
