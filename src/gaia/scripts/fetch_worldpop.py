import os
import sys
import math
from typing import List
import numpy as np
import rasterio
import pandas as pd
import geopandas as gpd
import argparse
import logging
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from rasterio import windows
from rasterio.transform import from_bounds
from rasterstats import zonal_stats

# --- UPDATED CONSTANTS ---
INDICATORS = {
    "total_pop": {
        "ages": [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80],
        "sexes": ["f", "m"],
    },
    "female_pop": {
        "ages": [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80],
        "sexes": ["f"],
    },
    "children_u5": {"ages": [0, 1], "sexes": ["f", "m"]},
    "female_u5": {"ages": [0, 1], "sexes": ["f"]},
    "elderly": {"ages": [65, 70, 75, 80], "sexes": ["f", "m"]},
    "pop_u15": {"ages": [0, 1, 5, 10], "sexes": ["f", "m"]},
    "female_u15": {"ages": [0, 1, 5, 10], "sexes": ["f"]},
    # 1. Women of Reproductive Age (15-49 years old)
    "wra_pop": {"ages": [15, 20, 25, 30, 35, 40, 45], "sexes": ["f"]},
    # 2. Dependency Ratio Components (Calculated down in aggregate workflow)
    "dep_dependents": {"ages": [0, 1, 5, 10, 65, 70, 75, 80], "sexes": ["f", "m"]},
    "dep_working": {
        "ages": [15, 20, 25, 30, 35, 40, 45, 50, 55, 60],
        "sexes": ["f", "m"],
    },
}

BASE_URL = "https://data.worldpop.org/GIS"
POP_TIMEFRAME = "Global_2015_2030"
RELEASE = "R2025A"
YEAR = "2030"
DOWNLOAD_WORKERS = 8
# Downsample merged indicator rasters to this resolution (m) — ~10x less memory,
# safe because every consumer sums per admin unit.
TARGET_RESOLUTION = 1000  # meters
# -------------------------


def download_url(url, dest_path):
    from gaia.scripts.download_utils import download_file

    download_file(url, dest_path)


def _downsample_sum(path: str, scale: int) -> np.ndarray:
    """Sum blocks of scale×scale source pixels, treating nodata as 0.

    The output grid is ceil(size/scale) cells covering the whole source extent
    (edge blocks are partial). Reads in row chunks so memory stays bounded.
    """
    with rasterio.open(path) as src:
        out_height = (src.height + scale - 1) // scale
        out_width = (src.width + scale - 1) // scale
        acc = np.zeros((out_height, out_width), dtype="float64")
        block_rows = max(scale, 512)
        block_rows -= block_rows % scale
        for start_row in range(0, src.height, block_rows):
            n_rows = min(block_rows, src.height - start_row)
            arr = (
                src.read(
                    1,
                    window=windows.Window(0, start_row, src.width, n_rows),
                    masked=True,
                )
                .filled(0)
                .astype("float64")
            )
            pad_h = (scale - arr.shape[0] % scale) % scale
            pad_w = (scale - arr.shape[1] % scale) % scale
            if pad_h or pad_w:
                arr = np.pad(
                    arr, ((0, pad_h), (0, pad_w)), mode="constant", constant_values=0
                )
            n_y = arr.shape[0] // scale
            n_x = arr.shape[1] // scale
            block_sum = arr.reshape(n_y, scale, n_x, scale).sum(axis=(1, 3))
            y0 = start_row // scale
            acc[y0 : y0 + n_y, :n_x] += block_sum
        return acc.astype("float32")


def _resolution_meters(src) -> float:
    """Ground resolution of the source raster in meters (across columns)."""
    crs = src.crs
    if crs and crs.is_geographic:
        lat = (src.bounds.top + src.bounds.bottom) / 2.0
        lat = max(-60.0, min(60.0, lat))
        return abs(src.res[0]) * 111320.0 * math.cos(math.radians(lat))
    return abs(src.res[0])


def merge_and_sum_rasters(raster_paths: List[str], out_path: str, context_log):
    if not raster_paths:
        raise ValueError("No rasters passed for merging!")
    with rasterio.open(raster_paths[0]) as src0:
        meta = src0.meta.copy()
        scale = max(1, round(TARGET_RESOLUTION / _resolution_meters(src0)))
        out_height = (src0.height + scale - 1) // scale
        out_width = (src0.width + scale - 1) // scale
        dst_transform = from_bounds(
            src0.bounds.left,
            src0.bounds.bottom,
            src0.bounds.right,
            src0.bounds.top,
            out_width,
            out_height,
        )
        context_log.info(
            f"Summing {len(raster_paths)} rasters into {os.path.basename(out_path)} "
            f"({out_height}x{out_width} cells, {scale}x downsampling) ..."
        )
        data_sum = _downsample_sum(raster_paths[0], scale)
        context_log.info(f"  processed 1/{len(raster_paths)} rasters")
    for i, p in enumerate(raster_paths[1:], start=2):
        data_sum += _downsample_sum(p, scale)
        context_log.info(f"  processed {i}/{len(raster_paths)} rasters")
    meta.update(
        dtype="float32",
        count=1,
        compress="lzw",
        nodata=0,
        transform=dst_transform,
        height=out_height,
        width=out_width,
    )
    with rasterio.open(out_path, "w", **meta) as dst:
        dst.write(data_sum, 1)
    context_log.info(f"Wrote merged raster to {out_path}")


def _subtract_rasters(base_path: str, minus_path: str, out_path: str, context_log):
    """Write out_path = base - minus (float32), clamping negatives to 0.

    Used to derive dep_dependents from total_pop - dep_working. Because the
    block-sum downsample is linear and the dep age groups partition total_pop,
    this is equivalent (up to float32 rounding) to merging dep_dependents bins.
    """
    with rasterio.open(base_path) as base, rasterio.open(minus_path) as minus:
        if base.shape != minus.shape or base.bounds != minus.bounds:
            raise ValueError("Rasters to subtract must share shape and bounds")
        meta = base.meta.copy()
        meta.update(dtype="float32", count=1, compress="lzw", nodata=0)
        data = np.maximum(
            0.0,
            base.read(1).astype("float64") - minus.read(1).astype("float64"),
        )
        with rasterio.open(out_path, "w", **meta) as dst:
            dst.write(data.astype("float32"), 1)
    context_log.info(
        f"Wrote merged raster to {out_path} "
        f"(derived from {os.path.basename(base_path)} - {os.path.basename(minus_path)})"
    )


def _indicator_bin_paths(out_dir_raw: str, country: str, ind: dict) -> list[str]:
    return [
        os.path.join(out_dir_raw, f"{country}_{sex}_{age}_{YEAR}_constrained.tif")
        for sex in ind["sexes"]
        for age in ind["ages"]
    ]


def fetch_worldpop(country, context_log=None, worldpop_code=None):
    if context_log is None:
        logging.basicConfig(level=logging.INFO)
        context_log = logging.getLogger("worldpop")

    country = country.upper()
    worldpop_code = worldpop_code or country
    worldpop_code_low = worldpop_code.lower()

    out_dir = os.path.join("data", country, "Temporary")
    os.makedirs(out_dir, exist_ok=True)

    expected_outputs = [
        os.path.join(out_dir, f"{country}_pop_{ind_name}_{YEAR}_constrained.tif")
        for ind_name in INDICATORS.keys()
    ]
    if all(os.path.exists(path) for path in expected_outputs):
        context_log.info(f"[{country}] → indicators exist, skipping.")
        return expected_outputs

    out_dir_raw = os.path.join(out_dir, "worldpop_raw")
    os.makedirs(out_dir_raw, exist_ok=True)

    needed_bins = set()
    for ind in INDICATORS.values():
        for sex in ind["sexes"]:
            for age in ind["ages"]:
                needed_bins.add((sex, age))

    def _download_bin(bin_tuple):
        sex, age = bin_tuple
        age_str = str(age).zfill(2)
        fname = f"{worldpop_code_low}_{sex}_{age_str}_{YEAR}_CN_100m_{RELEASE}_v1.tif"
        url = f"{BASE_URL}/AgeSex_structures/{POP_TIMEFRAME}/{RELEASE}/{YEAR}/{worldpop_code}/v1/100m/constrained/{fname}"
        dest = os.path.join(
            out_dir_raw, f"{country}_{sex}_{age}_{YEAR}_constrained.tif"
        )
        if not os.path.exists(dest):
            context_log.info(f"[{country}] → downloading {sex}_{age_str} from {url}")
            download_url(url, dest)
        return url

    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as ex:
        futures = [ex.submit(_download_bin, b) for b in sorted(needed_bins)]
        try:
            for fut in as_completed(futures):
                fut.result()
        except Exception as e:
            context_log.error(f"Failed to download WorldPop bin: {e}")
            sys.exit(1)

    processed = []
    for ind_name, ind in INDICATORS.items():
        filtered_paths = _indicator_bin_paths(out_dir_raw, country, ind)
        merged_out = os.path.join(
            out_dir, f"{country}_pop_{ind_name}_{YEAR}_constrained.tif"
        )
        if ind_name == "dep_dependents":
            # dep_dependents + dep_working = total_pop, so derive dep_dependents
            # from total_pop minus a fresh dep_working merge (linearity makes this
            # equivalent to summing its 16 bins directly).
            base_out = os.path.join(
                out_dir, f"{country}_pop_total_pop_{YEAR}_constrained.tif"
            )
            dep_working_out = os.path.join(
                out_dir, f"{country}_pop_dep_working_{YEAR}_constrained.tif"
            )
            if not os.path.exists(dep_working_out):
                merge_and_sum_rasters(
                    _indicator_bin_paths(
                        out_dir_raw, country, INDICATORS["dep_working"]
                    ),
                    dep_working_out,
                    context_log,
                )
            if not os.path.exists(merged_out):
                _subtract_rasters(base_out, dep_working_out, merged_out, context_log)
            processed.append(merged_out)
            continue
        if not os.path.exists(merged_out):
            merge_and_sum_rasters(filtered_paths, merged_out, context_log)
        processed.append(merged_out)

    if os.path.exists(out_dir_raw):
        shutil.rmtree(out_dir_raw)
    return processed


def aggregate_worldpop_to_csv(
    country_code: str, admin_level="ADM2", context_log=None
) -> str:
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

    csv_path = os.path.join(
        output_dir, f"{country_code}_{admin_level}_demographics.csv"
    )
    if os.path.exists(csv_path):
        context_log.info(
            f"[{country_code}] → {admin_level} demographics CSV exists, skipping."
        )
        return csv_path

    # 1) Fetch indicators into data/{country}/Temporary
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

    # 3) Dynamically map indicator names to files from the updated dict keys
    tif_map = dict(zip(INDICATORS.keys(), tifs))

    results = pd.DataFrame()
    results[f"{admin_level}_PCODE"] = gdf[f"{admin_level}_PCODE"]
    results["ADM_PCODE"] = gdf[f"{admin_level}_PCODE"]

    # 4) Compute zonal sums
    for ind, path in tif_map.items():
        stats = zonal_stats(gdf, path, stats="sum", nodata=0)
        results[ind] = [s["sum"] for s in stats]

    # Ensure admin code is preserved
    admin_col = f"{admin_level}_PCODE"
    if "ADM_PCODE" not in results.columns and admin_col in results.columns:
        results["ADM_PCODE"] = gdf[admin_col]

    # Process and clean numeric values
    numeric_cols = [c for c in results.columns if c not in [admin_col, "ADM_PCODE"]]
    results[numeric_cols] = (
        results[numeric_cols]
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0)
        .replace([float("inf"), float("-inf")], 0)
    )

    # Round populations to absolute whole numbers before ratio logic
    results[numeric_cols] = results[numeric_cols].round(0).astype(int)

    # 5) Compute the mathematical Dependency Ratio
    # Formula: (Dependents / Working Age Population) * 100
    # Uses a safe division to avoid errors if a polygon has 0 working-age inhabitants
    results["dependency_ratio"] = (
        ((results["dep_dependents"] / results["dep_working"].replace(0, pd.NA)) * 100)
        .fillna(0)
        .round(2)
    )

    # Clean up the intermediate components used for calculation so they don't bloat the final CSV
    results.drop(columns=["dep_dependents", "dep_working"], inplace=True)

    # 6) Save CSV
    out_dir = os.path.join("data", country_code, "Output")
    os.makedirs(out_dir, exist_ok=True)
    csv_path = os.path.join(out_dir, f"{country_code}_{admin_level}_demographics.csv")
    results.to_csv(csv_path, index=False)

    return csv_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and aggregate WorldPop indicators for a country."
    )
    parser.add_argument("country", help="ISO3 country code (e.g., STP)")
    parser.add_argument(
        "--admin-level",
        default="ADM2",
        help="Administrative level for aggregation (default: ADM2)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("worldpop")

    country = args.country.upper()

    csv_file = aggregate_worldpop_to_csv(
        country_code=country, admin_level=args.admin_level, context_log=logger
    )

    print(f"\nGenerated CSV: {csv_file}")
