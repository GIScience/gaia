#!/usr/bin/env python3
"""
Drought exposure using the JRC GDO SPEI-6 monthly dataset.

Builds a global drought-class raster once (downloads/drought/spei6_drought_class_1991_2020.tif)
from the 1991-2020 SPEI-6 NetCDFs, then computes per-admin-unit exposure of
vulnerable populations and facilities for a country.

A month is a "drought event month" when it belongs to a run of at least 3
consecutive months with SPEI-6 below -1.5. Each pixel's drought-class is the
share of event months over the full 360-month window:
  class 1 = 0-5%, class 2 = >5-10%, class 3 = >10-15%, class 4 = >15%.

Output: data/{country_code}/Output/{country_code}_{admin_level}_drought_exposure.csv
"""

import os
import time
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import reproject
from rasterstats import zonal_stats

from gaia.scripts.download_utils import download_file
from gaia.scripts.fetch_facilities_ohsome_overpass import fetch_ohsome, fetch_overpass
from gaia.scripts.fetch_worldpop import INDICATORS, fetch_worldpop

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent

SPEI_BASE_URL = (
    "https://drought.emergency.copernicus.eu/data/Drought_Observatories_datasets/"
    "GDO_ERA5_Standardized_Precipitation_Evapotranspiration_Index_SPEI6/ver1-0-0/"
)
DROUGHT_YEARS = list(range(1991, 2021))
N_MONTHS = 12 * len(DROUGHT_YEARS)  # 360

SPEI_THRESHOLD = -1.5
MIN_CONSECUTIVE = 3
CLASS_BOUNDS = [0.05, 0.10, 0.15]  # share thresholds -> classes 1..4

DOWNLOAD_DIR = ROOT_DIR / "downloads" / "drought"
GLOBAL_LAYER_PATH = DOWNLOAD_DIR / "spei6_drought_class_1991_2020.tif"

FACILITY_CATEGORIES = ["education", "hospitals", "primary_healthcare"]
POP_INDICATORS = [
    "total_pop",
    "female_pop",
    "children_u5",
    "female_u5",
    "elderly",
    "pop_u15",
    "female_u15",
    "wra_pop",
    "dep_dependents",
    "dep_working",
]
DROUGHT_CLASSES = [1, 2, 3, 4]


def spei_filename(year: int) -> str:
    return f"spe06_m_gdo_{year}0101_{year}1201_m.nc"


def _ensure_spei_files(context) -> list[str]:
    """Download any missing yearly SPEI-6 NetCDFs into downloads/drought."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    paths = []
    for year in DROUGHT_YEARS:
        fname = spei_filename(year)
        dest = DOWNLOAD_DIR / fname
        if not dest.exists():
            context.info(f"Downloading SPEI-6 {fname} ...")
            download_file(f"{SPEI_BASE_URL}{fname}", str(dest))
        paths.append(str(dest))
    return paths


def _monthly_event_share(spei_files: list[str], context) -> np.ndarray:
    """
    Compute the share of drought event months (uint16/360) for the full grid.

    Streams one monthly layer at a time through a rolling window of
    MIN_CONSECUTIVE*2 - 1 months, so memory stays bounded regardless of the
    number of years processed. The series is padded with MIN_CONSECUTIVE - 1
    non-drought months on each side, so every real month is evaluated while
    events crossing the 1991-2020 window edges are truncated.
    """
    import collections

    event_months = np.zeros((720, 1440), dtype=np.uint16)
    window = collections.deque()  # holds up to MIN_CONSECUTIVE*2-1 binary layers
    pad = np.zeros((720, 1440), dtype=bool)
    n_pad = MIN_CONSECUTIVE - 1

    def _month_generator():
        for _ in range(n_pad):
            yield pad
        for path in spei_files:
            with rasterio.open(path) as src:
                for band in range(1, src.count + 1):
                    yield (src.read(band).astype(np.float32) < SPEI_THRESHOLD)
        for _ in range(n_pad):
            yield pad

    for _, binary in enumerate(_month_generator()):
        window.append(binary)
        if len(window) == MIN_CONSECUTIVE * 2 - 1:
            mid = MIN_CONSECUTIVE - 1
            b = list(window)
            # month mid is an event month when it belongs to a run of >= 3
            event = b[mid] & (
                (b[mid - 1] & b[mid - 2])
                | (b[mid - 1] & b[mid + 1])
                | (b[mid + 1] & b[mid + 2])
            )
            event_months += event.astype(np.uint16)
            window.popleft()

    return event_months


def _classify_event_months(event_months: np.ndarray) -> np.ndarray:
    """Map event-month counts to drought classes 0-4 (uint8)."""
    counts = event_months.astype(np.int32)
    bounds_months = [round(b * N_MONTHS) for b in CLASS_BOUNDS]  # [18, 36, 54]
    classes = np.searchsorted(bounds_months, counts, side="left").astype(np.uint8)
    classes[counts > 0] += 1
    return classes


def ensure_drought_class_raster(context) -> str:
    """Return the global drought-class raster, building it if missing."""
    if GLOBAL_LAYER_PATH.exists():
        context.info(f"Global drought class raster already exists: {GLOBAL_LAYER_PATH}")
        return str(GLOBAL_LAYER_PATH)

    context.info(
        f"Building global SPEI-6 drought class raster from {len(DROUGHT_YEARS)} "
        f"yearly NetCDFs ({N_MONTHS} months)..."
    )
    spei_files = _ensure_spei_files(context)
    event_months = _monthly_event_share(spei_files, context)
    classes = _classify_event_months(event_months)

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    meta = {
        "driver": "GTiff",
        "height": 720,
        "width": 1440,
        "count": 1,
        "dtype": "uint8",
        "crs": "EPSG:4326",
        "transform": rasterio.transform.from_origin(-180.0, 90.0, 0.25, 0.25),
        "compress": "lzw",
        "nodata": 0,
    }
    with rasterio.open(GLOBAL_LAYER_PATH, "w", **meta) as dst:
        dst.write(classes, 1)

    dist = pd.Series(classes.ravel()).value_counts().sort_index()
    context.info(
        f"Global drought class raster saved: {GLOBAL_LAYER_PATH} "
        f"(class distribution: {dist.to_dict()})"
    )
    return str(GLOBAL_LAYER_PATH)


def _warp_drought_to_reference(
    context, global_path: str, ref_tif: str, out_path
) -> str:
    """Warp the 0.25° global drought raster onto the WorldPop reference grid."""
    with rasterio.open(global_path) as src_g, rasterio.open(ref_tif) as src_ref:
        meta = src_ref.meta.copy()
        dst_arr = np.zeros((src_ref.height, src_ref.width), dtype=np.uint8)
        reproject(
            source=rasterio.band(src_g, 1),
            destination=dst_arr,
            src_crs=src_g.crs,
            src_transform=src_g.transform,
            dst_crs=src_ref.crs,
            dst_transform=src_ref.transform,
            dst_shape=(src_ref.height, src_ref.width),
            resampling=Resampling.nearest,
        )
    meta.update(dtype="uint8", count=1, compress="lzw", nodata=0)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with rasterio.open(out_path, "w", **meta) as dst:
        dst.write(dst_arr, 1)
    context.info(f"Warped drought classes to WorldPop grid: {out_path}")
    return str(out_path)


def calculate_drought_exposure(
    context,
    country_code: str,
    admin_level="ADM2",
    api_choice="ohsome-api",
):
    country_code = country_code.upper()
    admin_level = admin_level.upper()
    base_path = Path(f"data/{country_code}")
    temp_dir = base_path / "Temporary"
    temp_dir.mkdir(parents=True, exist_ok=True)
    out_csv = (
        base_path / "Output" / f"{country_code}_{admin_level}_drought_exposure.csv"
    )
    if out_csv.exists():
        context.info(f"Drought exposure CSV already exists, skipping: {out_csv}")
        return str(out_csv)

    t_start = time.time()
    context.info("=" * 60)
    context.info(f"Starting drought exposure for {country_code} {admin_level}")
    context.info("=" * 60)

    boundary_file = base_path / f"{country_code}_{admin_level}.geojson"
    if not boundary_file.exists():
        raise FileNotFoundError(f"Boundary file not found: {boundary_file}")
    gdf_admin = gpd.read_file(boundary_file).to_crs("EPSG:4326")
    context.info(
        f"[{time.time() - t_start:.1f}s] Loaded admin boundary "
        f"with {len(gdf_admin)} units"
    )

    context.info("[1/5] Ensuring the global drought class raster exists...")
    t0 = time.time()
    global_raster_path = ensure_drought_class_raster(context)
    context.info(f"[{time.time() - t0:.1f}s] Global drought raster ready")

    context.info("[2/5] Ensuring demographic rasters exist...")
    t0 = time.time()
    indicator_tifs = fetch_worldpop(country_code)
    full_tif_map = dict(zip(INDICATORS.keys(), indicator_tifs))
    tif_map = {k: full_tif_map[k] for k in POP_INDICATORS}
    context.info(
        f"[{time.time() - t0:.1f}s] WorldPop rasters ready ({len(tif_map)} indicators)"
    )

    context.info("[3/5] Ensuring facility raw geometries exist...")
    t0 = time.time()
    api_choice = api_choice.lower()
    if api_choice == "ohsome-api":
        fetch_ohsome(context, boundary_file, base_path, country_code, admin_level)
    elif api_choice == "overpass":
        fetch_overpass(context, boundary_file, base_path, country_code, admin_level)
    else:
        context.warning(
            f"No valid API configured for facilities_asset (got '{api_choice}')"
        )
        return None
    context.info(f"[{time.time() - t0:.1f}s] Facilities ready")

    context.info("[4/5] Warping drought classes to WorldPop grid...")
    t0 = time.time()
    drought_raster_path = _warp_drought_to_reference(
        context,
        global_raster_path,
        indicator_tifs[0],
        temp_dir / f"{country_code}_drought_exposure.tif",
    )
    with rasterio.open(drought_raster_path) as src:
        drought_raster = src.read(1).astype(np.uint8)
        raster_crs = src.crs
    context.info(f"[{time.time() - t0:.1f}s] Drought raster done")

    df = pd.DataFrame({f"{admin_level}_PCODE": gdf_admin[f"{admin_level}_PCODE"]})
    df["ADM_PCODE"] = df[f"{admin_level}_PCODE"]

    context.info(
        f"[5/5] Computing population exposure across {len(tif_map)} indicators "
        f"x {len(DROUGHT_CLASSES)} classes..."
    )
    t0 = time.time()
    class_masks = {
        cls: (drought_raster == cls).astype(np.float32) for cls in DROUGHT_CLASSES
    }
    for indicator, pop_raster_path in tif_map.items():
        with rasterio.open(pop_raster_path) as src_pop:
            pop_raster = src_pop.read(1)
            transform = src_pop.transform
        for cls in DROUGHT_CLASSES:
            exposed_pop = (pop_raster * class_masks[cls]).astype(np.float32)
            stats = zonal_stats(
                gdf_admin,
                exposed_pop,
                affine=transform,
                stats="sum",
                nodata=0,
            )
            df[f"spei6_{indicator}_class{cls}"] = [
                round(s["sum"] or 0, 0) for s in stats
            ]
    context.info(f"  Population exposure total: [{time.time() - t0:.1f}s]")

    for cls in DROUGHT_CLASSES:
        dep_num = df[f"spei6_dep_dependents_class{cls}"]
        dep_den = df[f"spei6_dep_working_class{cls}"].replace(0, pd.NA)
        df[f"spei6_dependency_ratio_class{cls}"] = (
            ((dep_num / dep_den) * 100).fillna(0).round(2)
        )
        df.drop(
            columns=[
                f"spei6_dep_dependents_class{cls}",
                f"spei6_dep_working_class{cls}",
            ],
            inplace=True,
        )

    context.info("Computing facility exposure...")
    t0 = time.time()
    for category in FACILITY_CATEGORIES:
        filepath = base_path / f"Temporary/{country_code}_{category}_raw.geojson"
        if not filepath.exists():
            context.info(f"  No {category} file found, skipping")
            continue
        facilities = gpd.read_file(filepath)
        if facilities.empty:
            context.info(f"  {category} file is empty, skipping")
            continue
        facilities = facilities.to_crs(raster_crs)
        facilities["geometry"] = facilities.geometry.centroid
        coords = [(x, y) for x, y in zip(facilities.geometry.x, facilities.geometry.y)]
        with rasterio.open(drought_raster_path) as src:
            values = [v for v in src.sample(coords)]
        facilities["drought_class"] = [v[0] for v in values]

        joined = gpd.sjoin(
            facilities,
            gdf_admin[[f"{admin_level}_PCODE", "geometry"]],
            how="inner",
            predicate="within",
        )

        total_facilities = joined.groupby(f"{admin_level}_PCODE").size().to_dict()
        for cls in DROUGHT_CLASSES:
            grouped = (
                joined[joined["drought_class"] == cls]
                .groupby(f"{admin_level}_PCODE")
                .size()
                .reset_index(name=f"spei6_{category}_count_class{cls}")
            )
            df = df.merge(grouped, on=f"{admin_level}_PCODE", how="left")
            count_col = f"spei6_{category}_count_class{cls}"
            perc_col = f"spei6_{category}_perc_class{cls}"
            df[count_col] = df[count_col].fillna(0).astype(int)
            df[perc_col] = (
                df[count_col]
                / df[f"{admin_level}_PCODE"].map(total_facilities).fillna(1)
                * 100
            ).round(0)
        context.info(f"  [{time.time() - t0:.1f}s] {category}")

    numeric_cols = [
        c
        for c in df.select_dtypes(include=["float", "int"]).columns
        if "dependency_ratio" not in c
    ]
    df[numeric_cols] = df[numeric_cols].fillna(0).round(0).astype(int)

    output_dir = base_path / "Output"
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    context.info(
        f"[{time.time() - t_start:.1f}s] DONE — Drought exposure "
        f"CSV saved to: {out_csv}"
    )
    context.info(f"  Columns: {len(df.columns)}, Rows: {len(df)}")
    return str(out_csv)


if __name__ == "__main__":
    import argparse

    class PrintLogger:
        def info(self, msg):
            print(f"INFO: {msg}")

        def warning(self, msg):
            print(f"WARNING: {msg}")

    parser = argparse.ArgumentParser(
        description="Process drought exposure and vulnerable populations/facilities."
    )
    parser.add_argument("country_code", help="ISO3 country code, e.g., MMR")
    parser.add_argument(
        "admin_level",
        nargs="?",
        default="ADM2",
        help="Administrative level, default ADM2",
    )
    parser.add_argument(
        "--api",
        default="ohsome-api",
        help="Facilities API: ohsome-api or overpass",
    )
    args = parser.parse_args()

    calculate_drought_exposure(
        PrintLogger(), args.country_code.upper(), args.admin_level.upper(), args.api
    )
