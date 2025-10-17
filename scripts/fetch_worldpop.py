import os
import sys
from typing import List
import requests
import rasterio
import pandas as pd
import geopandas as gpd
import argparse
import logging
import shutil
from rasterstats import zonal_stats

# Define indicators directly
INDICATORS = {
    "female_pop": {"ages": [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80], "sexes": ["f"]},
    "children_u5": {"ages": [0, 1], "sexes": ["f", "m"]},
    "female_u5": {"ages": [0, 1], "sexes": ["f"]},
    "elderly": {"ages": [65, 70, 75, 80], "sexes": ["f", "m"]},
    "pop_u15": {"ages": [0, 1, 5, 10], "sexes": ["f", "m"]},
    "female_u15": {"ages": [0, 1, 5, 10], "sexes": ["f"]},
}

BASE_URL = "https://data.worldpop.org/GIS"
POP_TIMEFRAME = "Global_2000_2020_Constrained"
YEAR = "2020"


def download_url(url, dest_path):
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    with open(dest_path, "wb") as fp:
        for chunk in resp.iter_content(1024 * 1024):
            fp.write(chunk)


def merge_and_sum_rasters(raster_paths: List[str], out_path: str, context_log):
    """
    Open each path in `raster_paths`, read band 1 into memory, sum them,
    and write a single‐band float32 GeoTIFF to `out_path`.
    """
    if not raster_paths:
        raise ValueError("No rasters passed for merging!")

    with rasterio.open(raster_paths[0]) as src0:
        meta = src0.meta.copy()
        data_sum = src0.read(1, masked=True).filled(0).astype("float64")

    for p in raster_paths[1:]:
        with rasterio.open(p) as src:
            arr = src.read(1, masked=True).filled(0).astype("float64")
            data_sum += arr

    meta.update(dtype="float32", count=1, compress="lzw", nodata=0)

    with rasterio.open(out_path, "w", **meta) as dst:
        dst.write(data_sum.astype("float32"), 1)

    context_log.info(f"Wrote merged raster to {out_path}")


def fetch_worldpop(country, context_log=None, worldpop_code=None):
    """
    Download WorldPop rasters for a country and aggregate into indicator rasters.
    Produces 6 GeoTIFFs: one per indicator (no total population).
    """
    if context_log is None:
        logging.basicConfig(level=logging.INFO)
        context_log = logging.getLogger("worldpop")

    country = country.upper()

    if not worldpop_code:
        worldpop_code = country
        worldpop_code_low = country.lower()
    else:
        worldpop_code_low = worldpop_code.lower()

    # Force output directory to Temporary
    out_dir = os.path.join("data", country, "Temporary")
    os.makedirs(out_dir, exist_ok=True)

    # Pre-check: if all 6 indicator rasters already exist, skip
    expected_outputs = [
        os.path.join(out_dir, f"{country}_pop_{ind_name}_{YEAR}_constrained.tif")
        for ind_name in INDICATORS.keys()
    ]
    if all(os.path.exists(path) for path in expected_outputs):
        context_log.info(f"[{country}] → all {len(expected_outputs)} WorldPop indicators already exist, skipping download.")
        return expected_outputs
    
    out_dir_raw = os.path.join(out_dir, "worldpop_raw")
    os.makedirs(out_dir_raw, exist_ok=True)

    downloaded = []

    # 1) Download needed age/sex rasters
    needed_bins = set()
    for ind in INDICATORS.values():
        for sex in ind["sexes"]:
            for age in ind["ages"]:
                needed_bins.add((sex, age))

    for sex, age in needed_bins:
        fname = f"{worldpop_code_low}_{sex}_{age}_{YEAR}_constrained.tif"
        url = f"{BASE_URL}/AgeSex_structures/{POP_TIMEFRAME}/{YEAR}/{worldpop_code}/{fname}"
        dest = os.path.join(out_dir_raw, f"{country}_{sex}_{age}_{YEAR}_constrained.tif")
        if not os.path.exists(dest):
            context_log.info(f"[{country}] → downloading {sex}_{age}")
            try:
                download_url(url, dest)
            except Exception as e:
                context_log.info(f"[ERROR] failed to download {url}: {e}")
                sys.exit(1)
        else:
            context_log.info(f"[{country}] → skipping existing {fname}")
        downloaded.append(dest)

    # 2) Aggregate indicators
    processed = []
    for ind_name, ind in INDICATORS.items():
        filtered_paths = [
            os.path.join(out_dir_raw, f"{country}_{sex}_{age}_{YEAR}_constrained.tif")
            for sex in ind["sexes"]
            for age in ind["ages"]
        ]
        merged_out = os.path.join(out_dir, f"{country}_pop_{ind_name}_{YEAR}_constrained.tif")
        if not os.path.exists(merged_out):
            context_log.info(f"[{country}] → processing indicator {ind_name}")
            try:
                merge_and_sum_rasters(filtered_paths, merged_out, context_log)
            except Exception as e:
                context_log.info(f"[ERROR] failed to process {ind_name}: {e}")
                sys.exit(1)
        else:
            context_log.info(f"[{country}] → skipping existing {ind_name}")
        processed.append(merged_out)


    # 3) Delete raw folder
    if os.path.exists(out_dir_raw):
        shutil.rmtree(out_dir_raw)
        context_log.info(f"[{country}] → deleted raw folder: {out_dir_raw}")

    context_log.info(f"\n✔ ∙ {len(processed)} indicators saved under {out_dir}")
    return processed


def aggregate_worldpop_to_csv(country_code: str, admin_level="ADM2", context_log=None) -> str:
    """
    Download WorldPop indicators for a country and save CSV.
    """
    temp_dir = os.path.join("data", country_code, "Temporary")
    os.makedirs(temp_dir, exist_ok=True)

    output_dir = os.path.join("data", country_code, "Output")
    os.makedirs(output_dir, exist_ok=True)
    if context_log is None:
        logging.basicConfig(level=logging.INFO)
        context_log = logging.getLogger("worldpop")

    # 1) Fetch indicators (6 GeoTIFFs) into data/{country}/Temporary
    tifs = fetch_worldpop(country=country_code, context_log=context_log)

    # 2) Load ADM polygons
    adm_path = f"data/{country_code}/{country_code}_{admin_level}.geojson"
    gdf = gpd.read_file(adm_path)

    expected_column = f"{admin_level}_PCODE"
    if expected_column not in gdf.columns:
        raise ValueError(
            f"GeoJSON must contain column '{expected_column}' "
            f"(found: {gdf.columns.tolist()})"
        )
    # 3) Map indicator names to files
    indicators = ["female_pop","children_u5","female_u5","elderly","pop_u15","female_u15"]
    tif_map = dict(zip(indicators, tifs))

    results = pd.DataFrame()
    results[f"{admin_level}_PCODE"] = gdf[f"{admin_level}_PCODE"]

    # 4) Compute zonal sums
    for ind, path in tif_map.items():
        stats = zonal_stats(gdf, path, stats="sum", nodata=0)
        results[ind] = [s["sum"] for s in stats]

    # Round numeric columns and handle NaN or inf safely
    numeric_cols = results.columns.drop(f"{admin_level}_PCODE")

    # Replace non-finite values (NaN, inf) with 0 before conversion
    results[numeric_cols] = results[numeric_cols].apply(
        pd.to_numeric, errors="coerce"
    ).fillna(0).replace([float("inf"), float("-inf")], 0)

    results[numeric_cols] = results[numeric_cols].round(0).astype(int)

    # 5) Save CSV
    out_dir = os.path.join("data", country_code, "Output")
    os.makedirs(out_dir, exist_ok=True)
    csv_path = os.path.join(out_dir, f"{country_code}_{admin_level}_demographics.csv")
    results.to_csv(csv_path, index=False)

    return csv_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and aggregate WorldPop indicators for a country.")
    parser.add_argument("country", help="ISO3 country code (e.g., STP)")
    parser.add_argument(
        "--admin-level",
        default="ADM2",
        help="Administrative level for aggregation (default: ADM2)",
    )
    args = parser.parse_args()

    # Simple logger
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("worldpop")

    country = args.country.upper()

    # Execute full workflow: download + aggregate to CSV
    csv_file = aggregate_worldpop_to_csv(
        country_code=country,
        admin_level=args.admin_level,
        context_log=logger
    )

    print(f"\nGenerated CSV: {csv_file}")