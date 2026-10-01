#!/usr/bin/env python3
"""
Generates a global drought-class raster from the Copernicus Global Drought
Observatory (GDO) SPEI-6 monthly time series and computes vulnerable
population and facility exposure per admin unit, per drought class, the
same way flood and cyclone exposure are computed.

A month is a "drought event month" when it belongs to a run of at least
`min_consecutive_months` consecutive months with SPEI-6 <= `spei_threshold`.
Each pixel's drought class is the share of event months over a rolling
30-year baseline (CLASS_WINDOW_MONTHS, ending at the latest month cached
locally): class 1 = >0-5%, class 2 = >5-10%, class 3 = >10-15%, class 4 =
>15%. Pixels with no qualifying event at all are class 0 (background/no
data). This mirrors Elias's reference scripts
(SPEI-6_drought_mask_calculation.py / SPEI-6_persistent_drought_events.py),
generalized to a single global pass instead of a per-country clip.

Data source: https://drought.emergency.copernicus.eu/tumbo/gdo/download/ (the
interactive portal); the underlying yearly archives are also served directly,
unauthenticated, from GDO_INDEX_URL below, and are auto-downloaded into
<repo_root>/downloads/drought/spe06_m_gdo_<year_start>_<year_end>_m.zip as needed.
Each yearly archive contains one global GeoTIFF per available month
(spe06_m_gdo_YYYYMMDD_m_100_z0N.tif — EPSG:4326, 0.25°, continuous SPEI-6
values).

Output CSV: {country_code}_{admin_level}_drought_exposure.csv
"""

import re
import zipfile
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import requests
from rasterio.enums import Resampling
from rasterio.warp import reproject
from rasterstats import zonal_stats

from gaia.defs.constants import (
    DEFAULT_MIN_CONSECUTIVE_MONTHS,
    DEFAULT_SPEI_THRESHOLD,
    FACILITY_CATEGORIES,
    POP_INDICATORS,
    REPO_ROOT,
)
from gaia.scripts.download_utils import download_file
from gaia.scripts.fetch_facilities_ohsome_overpass import fetch_ohsome, fetch_overpass
from gaia.scripts.fetch_worldcover import DEFAULT_CROPS_YEAR, crop_exposure_km2
from gaia.scripts.fetch_worldpop import INDICATORS, fetch_worldpop

GDO_DATA_DIR = REPO_ROOT / "downloads" / "drought"
GDO_EXTRACT_DIR = GDO_DATA_DIR / "extracted"

# Unauthenticated static directory serving the same yearly archives as the
# interactive https://drought.emergency.copernicus.eu/tumbo/gdo/download/
# portal (confirmed via `curl` — plain Apache-style autoindex, no auth).
GDO_INDEX_URL = (
    "https://drought.emergency.copernicus.eu/data/Drought_Observatories_datasets/"
    "GDO_ERA5_Standardized_Precipitation_Evapotranspiration_Index_SPEI6/ver1-0-0/"
)
_REMOTE_ZIP_RE = re.compile(r'href="(spe06_m_gdo_\d{8}_\d{8}_m\.zip)"')

# Rolling 30-year baseline used as the denominator for "share of
# drought-event months": the most recent CLASS_WINDOW_MONTHS months
# available in the local GDO cache, matching Elias's reference
# (SPEI-6_myanmar_clipped covered "the last 30 years" as of when he ran it).
# This drifts forward as more recent months get cached, which is intentional
# here — it's what reproduces his numbers, unlike a fixed calendar baseline.
CLASS_WINDOW_MONTHS = 360

# Event-month-share bounds -> classes 1..4 (class 0 = no qualifying event).
CLASS_BOUNDS = [0.05, 0.10, 0.15]
DROUGHT_CLASSES = [1, 2, 3, 4]
CLASS_LABELS = {
    1: "0-0.05",
    2: "0.05-0.1",
    3: "0.1-0.15",
    4: "0.15+",
}

_RASTER_NAME_RE = re.compile(r"spe06_m_gdo_(\d{8})_m_100_z(\d+)\.tif$")


def _iter_available_rasters():
    """Yield (date_str, zone, zip_path, member) for every monthly SPEI-6
    raster in the local GDO cache, across all yearly zip archives."""
    for zip_path in sorted(GDO_DATA_DIR.glob("spe06_m_gdo_*.zip")):
        with zipfile.ZipFile(zip_path) as zf:
            for member in zf.namelist():
                match = _RASTER_NAME_RE.search(member)
                if match:
                    date_str, zone = match.groups()
                    yield date_str, int(zone), zip_path, member


def _all_dated_rasters():
    """Return every available month as a sorted list of
    (date_str, zip_path, member), one entry per date. When a month has
    multiple zones (z01, z02, ...), the highest zone is preferred — GDO
    reprocesses a preliminary month into a later, validated zone once the
    underlying climate reanalysis lands."""
    if not GDO_DATA_DIR.is_dir():
        raise FileNotFoundError(
            f"GDO drought data directory not found: {GDO_DATA_DIR}. Download "
            "monthly SPEI-6 archives from "
            "https://drought.emergency.copernicus.eu/tumbo/gdo/download/ "
            f"into {GDO_DATA_DIR}."
        )

    best = {}
    for date_str, zone, zip_path, member in _iter_available_rasters():
        if date_str not in best or zone > best[date_str][0]:
            best[date_str] = (zone, zip_path, member)

    if not best:
        raise FileNotFoundError(
            f"No SPEI-6 rasters found under {GDO_DATA_DIR}. Expected files "
            "named like spe06_m_gdo_20240101_m_100_z02.tif inside "
            "spe06_m_gdo_<start>_<end>_m.zip archives."
        )

    return [
        (date_str, zip_path, member)
        for date_str, (_, zip_path, member) in sorted(best.items())
    ]


def _extract_raster(zip_path: Path, member: str) -> Path:
    GDO_EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
    extracted_path = GDO_EXTRACT_DIR / member
    if not extracted_path.exists():
        with zipfile.ZipFile(zip_path) as zf, zf.open(member) as src:
            extracted_path.write_bytes(src.read())
    return extracted_path


def ensure_gdo_archives(context) -> None:
    """
    Sync downloads/drought/ against GDO_INDEX_URL: download any yearly SPEI-6 zip
    archive that isn't cached locally yet. The current (in-progress) year's
    archive grows month by month under the same filename prefix but a
    changing end-date suffix (e.g. ..._20260701_m.zip -> ..._20260801_m.zip)
    — when a newer version of that file appears, it's downloaded and the
    superseded local copy is removed so stale partial-year data doesn't
    linger in the cache.

    Best-effort: if the remote index can't be reached (offline, portal down),
    logs a warning and falls back to whatever is already cached locally.
    """
    try:
        resp = requests.get(GDO_INDEX_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        context.warning(
            f"Could not reach GDO archive index ({GDO_INDEX_URL}): {e}. "
            "Falling back to the local downloads/drought/ cache as-is."
        )
        return

    remote_files = sorted(set(_REMOTE_ZIP_RE.findall(resp.text)))
    if not remote_files:
        context.warning(
            f"No SPEI-6 zip archives found at {GDO_INDEX_URL}; the page "
            "layout may have changed. Falling back to the local cache."
        )
        return

    GDO_DATA_DIR.mkdir(parents=True, exist_ok=True)
    local_names = {p.name for p in GDO_DATA_DIR.glob("spe06_m_gdo_*.zip")}

    downloaded = []
    for fname in remote_files:
        if fname in local_names:
            continue

        start_date = fname.split("_")[3]
        for stale in GDO_DATA_DIR.glob(f"spe06_m_gdo_{start_date}_*_m.zip"):
            context.info(f"Removing superseded SPEI-6 archive: {stale.name}")
            stale.unlink()

        context.info(f"Downloading new SPEI-6 archive: {fname} ...")
        download_file(GDO_INDEX_URL + fname, str(GDO_DATA_DIR / fname))
        downloaded.append(fname)

    if downloaded:
        context.info(f"Downloaded {len(downloaded)} SPEI-6 archive(s): {downloaded}")
    else:
        context.info("Local SPEI-6 archive cache is already up to date.")


def _expected_months(window_start: str, window_end: str) -> int:
    start = datetime.strptime(window_start, "%Y%m%d")
    end = datetime.strptime(window_end, "%Y%m%d")
    return (end.year - start.year) * 12 + (end.month - start.month) + 1


def _shift_months(date_str: str, months_back: int) -> str:
    """Return the date `months_back` months before `date_str` (both the
    1st of their respective month), as YYYYMM01."""
    dt = datetime.strptime(date_str, "%Y%m%d")
    total = dt.year * 12 + (dt.month - 1) - months_back
    year, month = divmod(total, 12)
    return f"{year:04d}{month + 1:02d}01"


def _check_monthly_continuity(context, dated_rasters, window_start, window_end):
    """Warn about missing months so the event-month-share denominator isn't
    silently computed over an incomplete baseline."""
    expected = _expected_months(window_start, window_end)
    found = len(dated_rasters)
    if found != expected:
        context.warning(
            f"Expected {expected} months between {window_start} and "
            f"{window_end}, found {found} in the local GDO cache. The "
            "event-month-share denominator will use the actual count found, "
            "which may skew class thresholds if months are missing."
        )

    present = {d[0] for d in dated_rasters}
    cursor = datetime.strptime(window_start, "%Y%m%d")
    end = datetime.strptime(window_end, "%Y%m%d")
    missing = []
    while cursor <= end:
        if cursor.strftime("%Y%m01") not in present:
            missing.append(cursor.strftime("%Y-%m"))
        cursor = (
            datetime(cursor.year + 1, 1, 1)
            if cursor.month == 12
            else datetime(cursor.year, cursor.month + 1, 1)
        )
    if missing:
        context.warning(f"Missing SPEI-6 months in baseline window: {missing}")


def build_drought_class_raster(
    context,
    spei_threshold: float = DEFAULT_SPEI_THRESHOLD,
    min_consecutive_months: int = DEFAULT_MIN_CONSECUTIVE_MONTHS,
    window_months: int = CLASS_WINDOW_MONTHS,
    auto_download: bool = True,
) -> str:
    """
    Build (or reuse a cached) global 0.25° drought-class raster from the
    most recent `window_months` months of SPEI-6 rasters available in the
    local GDO cache (a rolling baseline ending at whatever the latest cached
    month is, matching Elias's reference calculation).

    When `auto_download` is True (default), syncs downloads/drought/ against the
    remote GDO archive index first, so the rolling window reflects the
    latest month actually published, not just whatever was manually
    downloaded before. Set False to use only what's already cached locally
    (e.g. offline runs).

    Streams one month at a time (current run-length + cumulative event-month
    count per pixel), so memory stays bounded regardless of how many years
    are cached locally.
    """
    if auto_download:
        ensure_gdo_archives(context)

    all_rasters = _all_dated_rasters()
    window_end = all_rasters[-1][0]
    window_start = _shift_months(window_end, window_months - 1)

    dated_rasters = [
        d for d in all_rasters if window_start <= d[0] <= window_end
    ]
    if not dated_rasters:
        raise FileNotFoundError(
            f"No SPEI-6 rasters found in {GDO_DATA_DIR} within the baseline "
            f"window {window_start}-{window_end}."
        )
    _check_monthly_continuity(context, dated_rasters, window_start, window_end)
    n_months = len(dated_rasters)
    first_date, last_date = dated_rasters[0][0], dated_rasters[-1][0]

    thr_tag = f"{spei_threshold:g}".replace("-", "m").replace(".", "p")
    class_path = (
        GDO_DATA_DIR
        / f"spei6_drought_class_{window_start}_{window_end}_thr{thr_tag}_min{min_consecutive_months}.tif"
    )
    if class_path.exists():
        context.info(f"Global drought class raster already exists: {class_path}")
        return str(class_path)

    context.info(
        f"Building global SPEI-6 drought class raster from {n_months} monthly "
        f"rasters ({first_date} to {last_date}, threshold={spei_threshold}, "
        f"min_consecutive_months={min_consecutive_months})..."
    )

    meta = None
    current_run = None
    event_month_count = None
    valid_any = None

    for i, (date_str, zip_path, member) in enumerate(dated_rasters, start=1):
        raster_path = _extract_raster(zip_path, member)
        with rasterio.open(raster_path) as src:
            if meta is None:
                meta = src.meta.copy()
                shape = (src.height, src.width)
                current_run = np.zeros(shape, dtype=np.uint16)
                event_month_count = np.zeros(shape, dtype=np.uint32)
                valid_any = np.zeros(shape, dtype=bool)
            data = src.read(1).astype(np.float32)
            nodata = src.nodata

        valid = (
            np.isfinite(data)
            if nodata is None
            else (np.isfinite(data) & (data != nodata))
        )
        drought = valid & (data <= spei_threshold)

        current_run[drought] += 1
        run_break = ~drought
        ended_run = run_break & (current_run > 0)
        qualifying_event = ended_run & (current_run >= min_consecutive_months)
        event_month_count[qualifying_event] += current_run[qualifying_event]
        current_run[run_break] = 0
        valid_any |= valid

        if i % 60 == 0 or i == n_months:
            context.info(f"  [{i}/{n_months}] processed through {date_str}")

    # An event still running at the end of the series still qualifies.
    qualifying_at_end = current_run >= min_consecutive_months
    event_month_count[qualifying_at_end] += current_run[qualifying_at_end]

    bounds_months = [round(b * n_months) for b in CLASS_BOUNDS]
    classes = np.searchsorted(bounds_months, event_month_count, side="left").astype(
        np.uint8
    )
    classes[event_month_count > 0] += 1
    classes[~valid_any] = 0

    meta.update(dtype=rasterio.uint8, count=1, nodata=0, compress="lzw")
    GDO_DATA_DIR.mkdir(parents=True, exist_ok=True)
    with rasterio.open(class_path, "w", **meta) as dst:
        dst.write(classes, 1)

    dist = pd.Series(classes.ravel()).value_counts().sort_index()
    context.info(
        f"Global drought class raster saved: {class_path} "
        f"(class distribution: {dist.to_dict()})"
    )
    return str(class_path)


def _warp_drought_class_to_reference(
    context, global_class_path: str, reference_tif: str, out_path: Path
) -> str:
    """Warp the 0.25° global drought-class raster onto the WorldPop
    reference grid for a country (nearest-neighbor, since classes are
    categorical)."""
    with rasterio.open(global_class_path) as src_g, rasterio.open(
        reference_tif
    ) as src_ref:
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
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(out_path, "w", **meta) as dst:
        dst.write(dst_arr, 1)
    context.info(f"Warped drought classes to WorldPop grid: {out_path}")
    return str(out_path)


def calculate_drought_exposure(
    context,
    country_code: str,
    admin_level="ADM2",
    api_choice="ohsome-api",
    spei_threshold: float = DEFAULT_SPEI_THRESHOLD,
    min_consecutive_months: int = DEFAULT_MIN_CONSECUTIVE_MONTHS,
    crop_years: list | None = None,
):
    country_code = country_code.upper()
    admin_level = admin_level.upper()
    temp_dir = Path(f"data/{country_code}/Temporary")
    temp_dir.mkdir(parents=True, exist_ok=True)
    base_path = Path(f"data/{country_code}")
    out_csv = (
        base_path / "Output" / f"{country_code}_{admin_level}_drought_exposure.csv"
    )
    if out_csv.exists():
        context.info(f"Drought exposure CSV already exists, skipping: {out_csv}")
        return str(out_csv)

    boundary_file = base_path / f"{country_code}_{admin_level}.geojson"
    if not boundary_file.exists():
        raise FileNotFoundError(f"Boundary file not found: {boundary_file}")
    gdf_admin = gpd.read_file(boundary_file).to_crs("EPSG:4326")

    global_class_path = build_drought_class_raster(
        context, spei_threshold, min_consecutive_months
    )

    context.info(f"Ensuring demographic rasters exist in {temp_dir}...")
    indicator_tifs = fetch_worldpop(country_code)
    full_tif_map = dict(zip(INDICATORS.keys(), indicator_tifs))
    tif_map = {k: full_tif_map[k] for k in POP_INDICATORS}

    context.info(f"Ensuring facility raw geometries exist in {temp_dir}...")
    api_choice = api_choice.lower()
    if api_choice == "ohsome-api":
        fetch_ohsome(context, boundary_file, base_path, country_code, admin_level)
    elif api_choice == "overpass":
        fetch_overpass(context, boundary_file, base_path, country_code, admin_level)
    elif api_choice == "ohsome-parquet":
        context.info("Not implemented yet: ohsome-parquet")
        return None
    else:
        context.warning(
            f"No valid API configured for facilities_asset (got '{api_choice}')"
        )
        return None

    raster_path = _warp_drought_class_to_reference(
        context,
        global_class_path,
        indicator_tifs[0],
        temp_dir / f"{country_code}_drought_exposure.tif",
    )

    with rasterio.open(raster_path) as src:
        drought_raster = src.read(1).astype(np.uint8)
        raster_crs = src.crs

    # Initialize dataframe with admin PCODEs
    df = pd.DataFrame({f"{admin_level}_PCODE": gdf_admin[f"{admin_level}_PCODE"]})
    df["ADM_PCODE"] = df[f"{admin_level}_PCODE"]

    # --- Population exposure, per drought class ---
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
                gdf_admin, exposed_pop, affine=transform, stats="sum", nodata=0
            )
            df[f"spei6_{indicator}_class{cls}"] = [
                round(s["sum"] or 0, 0) for s in stats
            ]

    # Calculate dependency ratio per class and drop intermediate columns
    for cls in DROUGHT_CLASSES:
        dep_col_num = df[f"spei6_dep_dependents_class{cls}"]
        dep_col_den = df[f"spei6_dep_working_class{cls}"].replace(0, pd.NA)
        df[f"spei6_dependency_ratio_class{cls}"] = (
            ((dep_col_num / dep_col_den) * 100).fillna(0).round(2)
        )
        df.drop(
            columns=[
                f"spei6_dep_dependents_class{cls}",
                f"spei6_dep_working_class{cls}",
            ],
            inplace=True,
        )

    # --- Facility exposure, per drought class ---
    with rasterio.open(raster_path) as src:
        for category in FACILITY_CATEGORIES:
            filepath = base_path / f"Temporary/{country_code}_{category}_raw.geojson"
            if not filepath.exists():
                continue
            facilities = gpd.read_file(filepath)
            if facilities.empty:
                continue
            facilities = facilities.to_crs(raster_crs)
            facilities["geometry"] = facilities.geometry.centroid
            coords = [
                (x, y) for x, y in zip(facilities.geometry.x, facilities.geometry.y)
            ]
            values = [v[0] for v in src.sample(coords)]
            facilities["drought_class"] = values

            joined = gpd.sjoin(
                facilities,
                gdf_admin[[f"{admin_level}_PCODE", "geometry"]],
                how="inner",
                predicate="within",
            )

            total_facilities = joined.groupby(f"{admin_level}_PCODE").size().to_dict()
            for cls in DROUGHT_CLASSES:
                mask_cls = joined["drought_class"] == cls
                grouped = (
                    joined[mask_cls]
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

    # --- Cropland exposure, per drought class ---
    crop_year = crop_years[-1] if crop_years else DEFAULT_CROPS_YEAR
    crop_exposure = crop_exposure_km2(
        context,
        country_code,
        gdf_admin,
        {f"class{cls}": class_masks[cls] for cls in DROUGHT_CLASSES},
        year=crop_year,
    )
    if crop_exposure:
        for cls in DROUGHT_CLASSES:
            df[f"spei6_crops_km2_class{cls}"] = crop_exposure[f"class{cls}"]

    numeric_cols = [
        c
        for c in df.select_dtypes(include=["float", "int"]).columns
        if "dependency_ratio" not in c
    ]
    df[numeric_cols] = df[numeric_cols].fillna(0).round(0).astype(int)

    output_dir = base_path / "Output"
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    context.info(f"Drought exposure CSV saved to: {out_csv}")
    return str(out_csv)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Process drought exposure and vulnerable populations/facilities using GDO SPEI-6 data."
    )
    parser.add_argument("country_code", help="ISO3 country code, e.g., MMR")
    parser.add_argument(
        "admin_level",
        nargs="?",
        default="ADM2",
        help="Administrative level, default ADM2",
    )
    args = parser.parse_args()

    class PrintLogger:
        def info(self, msg):
            print(f"INFO: {msg}")

        def warning(self, msg):
            print(f"WARNING: {msg}")

    calculate_drought_exposure(
        PrintLogger(), args.country_code.upper(), args.admin_level.upper()
    )
