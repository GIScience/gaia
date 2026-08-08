"""
calculate_evacuatability.py

Calculates an "Evacuatability" indicator: the travel time (in minutes) for
at-risk (flooded / cyclone-affected) population to reach the nearest safe zone.

Uses MCP_Geometric from scikit-image for least-cost path analysis on a
friction surface, replicating GEE's cumulativeCost logic in pure Python.

Contains two main entry points for Dagster assets:
  - compute_flood_evacuability()
  - compute_cyclone_evacuability()

And a standalone CLI entry point:
  - process_evacuatability() / main()
"""

import os
import argparse
import glob
import re
import numpy as np
import geopandas as gpd
import pandas as pd
import rasterio
from rasterio.transform import Affine
from rasterio.windows import from_bounds, Window
from rasterio.warp import reproject, Resampling
from rasterstats import zonal_stats
from skimage.graph import MCP_Geometric
from pathlib import Path

# Default flood threshold (overridable per call via flood_threshold)
FLOOD_THRESHOLD = 0.3  # default: 30cm

# Remote friction surface COG URL
FRICTION_COG_URL = "https://hot.storage.heigit.org/heigit-hdx-public/risk_assessment_inputs/2020_motorized_friction_surface_cog.tif"

# Performance tuning
MAX_PIXELS_FOR_MCP = 10_000_000  # 10M pixels max before downsampling
TARGET_RESOLUTION_M = 500  # Target resolution when downsampling (meters)
MAX_MCP_SOURCES = 20000  # Max safe zone pixels to sample for MCP

# Demographic indicators to process (matching WorldPop naming)
DEMOGRAPHIC_INDICATORS = [
    "total_pop",
    "children_u5",
    "elderly",
    "female_pop",
    "female_u5",
    "female_u15",
    "pop_u15",
]


def find_population_rasters(temp_dir, country_code):
    pop_rasters = {}
    pattern = temp_dir / f"{country_code}_pop_*_2030_constrained.tif"
    for path in glob.glob(str(pattern)):
        filename = os.path.basename(path)
        match = re.search(rf"{country_code}_pop_(.+)_2030_constrained\.tif", filename)
        if match:
            indicator = match.group(1)
            pop_rasters[indicator] = Path(path)
    return pop_rasters


def downsample_array(arr, scale_factor, method="mean"):
    from scipy.ndimage import zoom

    if scale_factor <= 1:
        return arr
    scale_factor = int(round(scale_factor))
    if method == "sum":
        h, w = arr.shape
        new_h = (h // scale_factor) * scale_factor
        new_w = (w // scale_factor) * scale_factor
        trimmed = arr[:new_h, :new_w]
        reshaped = trimmed.reshape(
            new_h // scale_factor, scale_factor, new_w // scale_factor, scale_factor
        )
        result = np.nansum(reshaped, axis=(1, 3))
        return result.astype(arr.dtype)
    elif method == "mean":
        return zoom(arr, 1 / scale_factor, order=1)
    elif method == "max":
        return zoom(arr.astype(float), 1 / scale_factor, order=0) > 0.5
    else:
        return zoom(arr, 1 / scale_factor, order=0)


def upsample_array(arr, target_shape, method="bilinear"):
    from scipy.ndimage import zoom

    scale_y = target_shape[0] / arr.shape[0]
    scale_x = target_shape[1] / arr.shape[1]
    order = 1 if method == "bilinear" else 0
    return zoom(arr, (scale_y, scale_x), order=order)


def load_local_raster(path, band=1):
    with rasterio.open(path) as src:
        arr = src.read(band).astype(np.float32)
        profile = src.profile.copy()
        nodata = src.nodata
        bounds = src.bounds
        transform = src.transform
        crs = src.crs
    return arr, profile, nodata, bounds, transform, crs


def fetch_friction_window(
    bounds, target_crs, target_transform, target_shape, friction_url=FRICTION_COG_URL
):
    with rasterio.open(friction_url) as src:
        friction_crs = src.crs
        friction_nodata = src.nodata
        if target_crs != friction_crs:
            from rasterio.warp import transform_bounds

            src_bounds = transform_bounds(target_crs, friction_crs, *bounds)
        else:
            src_bounds = bounds
        window = from_bounds(*src_bounds, src.transform)
        col_off = max(0, int(window.col_off) - 5)
        row_off = max(0, int(window.row_off) - 5)
        width = min(src.width - col_off, int(window.width) + 10)
        height = min(src.height - row_off, int(window.height) + 10)
        window = Window(col_off, row_off, width, height)
        friction_raw = src.read(1, window=window).astype(np.float32)
        window_transform = src.window_transform(window)
    friction_aligned = np.zeros(target_shape, dtype=np.float32)
    reproject(
        source=friction_raw,
        destination=friction_aligned,
        src_transform=window_transform,
        src_crs=friction_crs,
        src_nodata=friction_nodata,
        dst_transform=target_transform,
        dst_crs=target_crs,
        dst_nodata=np.nan,
        resampling=Resampling.bilinear,
    )
    return friction_aligned


def create_cost_surface(friction_arr, hazard_arr, hazard_threshold=FLOOD_THRESHOLD):
    valid = ~np.isnan(hazard_arr)
    at_risk_mask = valid & (hazard_arr > hazard_threshold)
    safe_mask = valid & (hazard_arr <= hazard_threshold)
    no_data = np.isnan(hazard_arr) | (hazard_arr == 0)
    safe_mask = safe_mask | no_data
    cost_arr = friction_arr.copy()
    cost_arr[np.isnan(cost_arr)] = 1e6
    cost_arr[cost_arr <= 0] = 1e6
    return cost_arr, safe_mask, at_risk_mask


def create_cyclone_cost_surface(friction_arr, cyclone_arr):
    valid = ~np.isnan(cyclone_arr)
    at_risk_mask = valid & (cyclone_arr >= 1)
    safe_mask = valid & (cyclone_arr == 0)
    no_data = np.isnan(cyclone_arr)
    safe_mask = safe_mask | no_data
    cost_arr = friction_arr.copy()
    cost_arr[np.isnan(cost_arr)] = 1e6
    cost_arr[cost_arr <= 0] = 1e6
    return cost_arr, safe_mask, at_risk_mask


def calculate_travel_time_mcp(cost_arr, safe_mask, pixel_size_m):
    safe_indices = np.argwhere(safe_mask)
    if len(safe_indices) == 0:
        return np.full(cost_arr.shape, np.nan)
    if len(safe_indices) > MAX_MCP_SOURCES:
        rng = np.random.default_rng(42)
        indices = rng.choice(len(safe_indices), MAX_MCP_SOURCES, replace=False)
        safe_indices = safe_indices[indices]
    cost_scaled = cost_arr * pixel_size_m
    mcp = MCP_Geometric(cost_scaled, fully_connected=True)
    starts = [tuple(idx) for idx in safe_indices]
    cumulative_cost, traceback = mcp.find_costs(starts)
    travel_time = cumulative_cost.astype(np.float32)
    travel_time[travel_time >= 1e9] = np.nan
    return travel_time


def get_pixel_size_meters(transform, crs):
    pixel_x = abs(transform.a)
    pixel_y = abs(transform.e)
    if crs.is_geographic:
        meters_per_degree = 111000
        pixel_size_m = ((pixel_x + pixel_y) / 2) * meters_per_degree
    else:
        pixel_size_m = (pixel_x + pixel_y) / 2
    return pixel_size_m


def aggregate_by_admin(
    travel_time_arr,
    at_risk_mask,
    pop_rasters,
    gdf,
    admin_level,
    transform,
    crs,
    output_path,
):
    id_col = f"{admin_level}_PCODE"
    if id_col not in gdf.columns:
        raise ValueError(f"Column {id_col} not found in boundaries")
    if gdf.crs != crs:
        gdf = gdf.to_crs(crs)
    travel_time_at_risk = travel_time_arr.copy()
    travel_time_at_risk[~at_risk_mask] = np.nan
    tt_stats = zonal_stats(
        gdf,
        travel_time_at_risk.astype(np.float32),
        affine=transform,
        stats=["mean", "max", "median", "count"],
        nodata=np.nan,
    )
    pop_stats_all = {}
    for indicator, (pop_arr, pop_nodata) in pop_rasters.items():
        if at_risk_mask.shape != pop_arr.shape:
            mask_resized = (
                upsample_array(
                    at_risk_mask.astype(np.float32), pop_arr.shape, method="nearest"
                )
                > 0.5
            )
        else:
            mask_resized = at_risk_mask
        pop_at_risk = pop_arr.copy()
        pop_at_risk[~mask_resized] = 0
        pop_stats_all[indicator] = zonal_stats(
            gdf,
            pop_at_risk.astype(np.float32),
            affine=transform,
            stats=["sum"],
            nodata=0,
        )
    results = []
    for i, (idx, row) in enumerate(gdf.iterrows()):
        pcode = row[id_col]
        tt = tt_stats[i]
        record = {
            id_col: pcode,
            "evac_time_mean_min": round(tt["mean"], 1) if tt["mean"] else None,
            "evac_time_max_min": round(tt["max"], 1) if tt["max"] else None,
            "evac_time_median_min": round(tt["median"], 1) if tt["median"] else None,
            "pixels_at_risk": tt["count"] if tt["count"] else 0,
        }
        for indicator, pop_stats in pop_stats_all.items():
            pop = pop_stats[i]
            record[f"pop_at_risk_{indicator}"] = int(pop["sum"]) if pop["sum"] else 0
        results.append(record)
    df = pd.DataFrame(results)
    df.to_csv(output_path, index=False)
    return df


def process_evacuatability(
    country_code, admin_level="ADM2", rp="100", flood_threshold=None, context=None
):
    country_code = country_code.upper()
    admin_level = admin_level.upper()
    threshold = flood_threshold if flood_threshold else FLOOD_THRESHOLD
    log = context.info if context else print
    log(f"Processing evacuatability for {country_code}, {admin_level}, RP{rp}")
    log(f"  Flood threshold: {threshold}m")
    base_dir = Path(f"data/{country_code}")
    temp_dir = base_dir / "Temporary"
    output_dir = base_dir / "Output"
    output_dir.mkdir(parents=True, exist_ok=True)
    flood_path = temp_dir / f"{country_code}_flooded_RP{rp}.tif"
    boundary_path = base_dir / f"{country_code}_{admin_level}.geojson"
    output_csv = (
        output_dir / f"{country_code}_evacuatability_by_{admin_level}_RP{rp}.csv"
    )
    output_tif = output_dir / f"{country_code}_travel_time_RP{rp}.tif"
    if not flood_path.exists():
        raise FileNotFoundError(f"Flood raster not found: {flood_path}")
    if not boundary_path.exists():
        raise FileNotFoundError(f"Boundary file not found: {boundary_path}")
    pop_raster_paths = find_population_rasters(temp_dir, country_code)
    if not pop_raster_paths:
        raise FileNotFoundError(f"No population rasters found in {temp_dir}")
    log(
        f"  Found {len(pop_raster_paths)} demographic indicators: {list(pop_raster_paths.keys())}"
    )
    flood_arr, flood_profile, flood_nodata, bounds, transform, crs = load_local_raster(
        flood_path
    )
    if flood_nodata is not None:
        flood_arr[flood_arr == flood_nodata] = np.nan
    log("Loading population rasters...")
    pop_rasters = {}
    for indicator, path in pop_raster_paths.items():
        pop_arr, _, pop_nodata, _, _, _ = load_local_raster(path)
        if pop_nodata is not None:
            pop_arr[pop_arr == pop_nodata] = 0
        pop_rasters[indicator] = (pop_arr, pop_nodata)
    friction_arr = fetch_friction_window(
        bounds=(bounds.left, bounds.bottom, bounds.right, bounds.top),
        target_crs=crs,
        target_transform=transform,
        target_shape=flood_arr.shape,
    )
    pixel_size_m = get_pixel_size_meters(transform, crs)
    total_pixels = flood_arr.size
    original_shape = flood_arr.shape
    scale_factor = 1
    if total_pixels > MAX_PIXELS_FOR_MCP:
        scale_by_pixels = np.sqrt(total_pixels / MAX_PIXELS_FOR_MCP)
        scale_by_resolution = (
            TARGET_RESOLUTION_M / pixel_size_m
            if pixel_size_m < TARGET_RESOLUTION_M
            else 1
        )
        scale_factor = max(scale_by_pixels, scale_by_resolution)
        flood_arr_ds = downsample_array(flood_arr, scale_factor, method="mean")
        friction_arr_ds = downsample_array(friction_arr, scale_factor, method="mean")
        pixel_size_m_ds = pixel_size_m * scale_factor
    else:
        flood_arr_ds = flood_arr
        friction_arr_ds = friction_arr
        pixel_size_m_ds = pixel_size_m
    cost_arr, safe_mask, at_risk_mask_ds = create_cost_surface(
        friction_arr_ds, flood_arr_ds, threshold
    )
    if at_risk_mask_ds.sum() == 0:
        log("  No at-risk areas found. Creating empty output.")
        gdf = gpd.read_file(boundary_path)
        id_col = f"{admin_level}_PCODE"
        empty_data = {id_col: gdf[id_col]}
        empty_data["evac_time_mean_min"] = None
        empty_data["evac_time_max_min"] = None
        empty_data["evac_time_median_min"] = None
        empty_data["pixels_at_risk"] = 0
        for indicator in pop_rasters.keys():
            empty_data[f"pop_at_risk_{indicator}"] = 0
        df = pd.DataFrame(empty_data)
        df.to_csv(output_csv, index=False)
        empty_travel_time = np.full(original_shape, np.nan, dtype="float32")
        with rasterio.open(
            output_tif,
            "w",
            driver="GTiff",
            height=original_shape[0],
            width=original_shape[1],
            count=1,
            dtype="float32",
            crs=crs,
            transform=transform,
            nodata=np.nan,
            compress="lzw",
        ) as dst:
            dst.write(empty_travel_time, 1)
        return str(output_csv), str(output_tif)
    travel_time_ds = calculate_travel_time_mcp(cost_arr, safe_mask, pixel_size_m_ds)
    if scale_factor > 1:
        travel_time = upsample_array(travel_time_ds, original_shape)
        at_risk_mask = create_cost_surface(friction_arr, flood_arr, threshold)[2]
    else:
        travel_time = travel_time_ds
        at_risk_mask = at_risk_mask_ds
    travel_time_output = np.where(at_risk_mask, travel_time, np.nan)
    with rasterio.open(
        output_tif,
        "w",
        driver="GTiff",
        height=travel_time_output.shape[0],
        width=travel_time_output.shape[1],
        count=1,
        dtype="float32",
        crs=crs,
        transform=transform,
        nodata=np.nan,
        compress="lzw",
    ) as dst:
        dst.write(travel_time_output.astype("float32"), 1)
        dst.set_band_description(1, f"Travel time to safe zone (minutes), RP{rp}")
    gdf = gpd.read_file(boundary_path)
    df = aggregate_by_admin(
        travel_time_arr=travel_time,
        at_risk_mask=at_risk_mask,
        pop_rasters=pop_rasters,
        gdf=gdf,
        admin_level=admin_level,
        transform=transform,
        crs=crs,
        output_path=output_csv,
    )
    log(f"Evacuatability calculation complete for {country_code}")
    return str(output_csv), str(output_tif)


# =====================================================================
# Evacuability CSV — produces a standalone CSV with evacuability columns
# for both flood and cyclone (if data exists).
# =====================================================================


def compute_evacuability_csv(
    context,
    country_code: str,
    admin_level: str,
    rps: list[str] | None = None,
    flood_threshold: float = None,
) -> str | None:
    """
    Compute evacuability for flood (all RPs) and cyclone (if raster exists)
    and write a single CSV with all evacuability columns.

    CSV location: data/{country_code}/Output/{country_code}_{admin_level}_evacuability.csv

    Returns the path to the CSV, or None if no work was done.
    """
    threshold = flood_threshold if flood_threshold is not None else FLOOD_THRESHOLD
    log = context.info if hasattr(context, "info") else print

    if rps is None:
        rps = []

    country_code = country_code.upper()
    admin_level = admin_level.upper()
    base_path = Path(f"data/{country_code}")
    temp_dir = base_path / "Temporary"
    output_dir = base_path / "Output"
    boundary_path = base_path / f"{country_code}_{admin_level}.geojson"
    id_col = f"{admin_level}_PCODE"
    out_csv = output_dir / f"{country_code}_{admin_level}_evacuability.csv"

    if not boundary_path.exists():
        log(f"[{country_code}] Boundary not found: {boundary_path}")
        return None

    gdf = gpd.read_file(boundary_path)
    df = pd.DataFrame({id_col: gdf[id_col]})
    had_data = False

    # --- Flood evacuability ---
    # Analysis runs on a downsampled grid (bounded by MAX_PIXELS_FOR_MCP), so
    # the full-resolution ~100 m country raster is never materialized in RAM.
    # The flood raster is read already downsampled (rasterio out_shape) and the
    # friction window is fetched directly onto that same analysis grid.
    friction_ds = None
    friction_grid = None
    for rp in rps:
        flood_path = temp_dir / f"{country_code}_flooded_RP{rp}.tif"
        if not flood_path.exists():
            log(f"[{country_code}] Flood raster not found for RP{rp}, skipping")
            continue

        log(f"[{country_code}] Computing flood evacuability for RP{rp}...")

        # Open metadata only; derive the analysis grid from the raster profile.
        with rasterio.open(flood_path) as src:
            crs = src.crs
            transform = src.transform
            bounds = src.bounds
            flood_nodata = src.nodata
            full_shape = (src.height, src.width)

        pixel_size_m = get_pixel_size_meters(transform, crs)
        total_pixels = full_shape[0] * full_shape[1]
        scale_factor = 1
        if total_pixels > MAX_PIXELS_FOR_MCP:
            scale_by_pixels = np.sqrt(total_pixels / MAX_PIXELS_FOR_MCP)
            scale_by_resolution = (
                TARGET_RESOLUTION_M / pixel_size_m
                if pixel_size_m < TARGET_RESOLUTION_M
                else 1
            )
            scale_factor = max(scale_by_pixels, scale_by_resolution)

        ds_shape = (
            max(1, int(round(full_shape[0] / scale_factor))),
            max(1, int(round(full_shape[1] / scale_factor))),
        )
        ds_transform = transform * Affine.scale(
            full_shape[1] / ds_shape[1], full_shape[0] / ds_shape[0]
        )

        # Read the hazard already downsampled — never holds the full-res raster.
        with rasterio.open(flood_path) as src:
            hazard_ds = src.read(
                1, out_shape=ds_shape, resampling=Resampling.bilinear
            ).astype(np.float32)
        if flood_nodata is not None and not np.isnan(flood_nodata):
            hazard_ds[hazard_ds == flood_nodata] = np.nan

        # All RP flood rasters share the same country grid — fetch the friction
        # window once (on the analysis grid) and reuse it.
        grid = (bounds, ds_transform, crs, ds_shape)
        if friction_ds is None or grid != friction_grid:
            friction_ds = fetch_friction_window(
                bounds=(bounds.left, bounds.bottom, bounds.right, bounds.top),
                target_crs=crs,
                target_transform=ds_transform,
                target_shape=ds_shape,
            )
            friction_grid = grid

        pixel_size_ds = get_pixel_size_meters(ds_transform, crs)
        cost_arr, safe_mask, at_risk_ds = create_cost_surface(
            friction_ds, hazard_ds, threshold
        )

        if at_risk_ds.sum() == 0:
            log(f"[{country_code}] No at-risk areas for RP{rp}, setting nulls")
            df[f"RP{rp}_evac_time_minutes_mean"] = None
            df[f"RP{rp}_evac_time_minutes_max"] = None
            df[f"RP{rp}_evac_time_minutes_median"] = None
        else:
            travel_time_ds = calculate_travel_time_mcp(
                cost_arr, safe_mask, pixel_size_ds
            )

            travel_time_at_risk = travel_time_ds.copy()
            travel_time_at_risk[~at_risk_ds] = np.nan
            gdf_tt = gdf.to_crs(crs) if gdf.crs != crs else gdf

            tt_stats = zonal_stats(
                gdf_tt,
                travel_time_at_risk.astype(np.float32),
                affine=ds_transform,
                stats=["mean", "max", "median"],
                nodata=np.nan,
            )

            df[f"RP{rp}_evac_time_minutes_mean"] = [
                round(s["mean"], 1) if s.get("mean") else None for s in tt_stats
            ]
            df[f"RP{rp}_evac_time_minutes_max"] = [
                round(s["max"], 1) if s.get("max") else None for s in tt_stats
            ]
            df[f"RP{rp}_evac_time_minutes_median"] = [
                round(s["median"], 1) if s.get("median") else None for s in tt_stats
            ]

        had_data = True
        log(f"[{country_code}] Flood RP{rp} evacuability done")

    # --- Cyclone evacuability ---
    cyclone_raster_path = temp_dir / f"{country_code}_cyclone_exposure.tif"
    if cyclone_raster_path.exists():
        log(f"[{country_code}] Computing cyclone evacuability...")

        with rasterio.open(cyclone_raster_path) as src:
            cyclone_raster = src.read(1).astype(np.float32)
            raster_crs = src.crs
            cyclone_transform = src.transform
            cyclone_bounds = src.bounds
            cyclone_shape = cyclone_raster.shape
            cyclone_nodata = src.nodata

        if cyclone_nodata is not None:
            cyclone_raster[cyclone_raster == cyclone_nodata] = np.nan

        friction_arr = fetch_friction_window(
            bounds=(
                cyclone_bounds.left,
                cyclone_bounds.bottom,
                cyclone_bounds.right,
                cyclone_bounds.top,
            ),
            target_crs=raster_crs,
            target_transform=cyclone_transform,
            target_shape=cyclone_shape,
        )

        pixel_size_m = get_pixel_size_meters(cyclone_transform, raster_crs)
        total_pixels = cyclone_raster.size
        original_shape = cyclone_raster.shape
        scale_factor = 1

        if total_pixels > MAX_PIXELS_FOR_MCP:
            scale_by_pixels = np.sqrt(total_pixels / MAX_PIXELS_FOR_MCP)
            scale_by_resolution = (
                TARGET_RESOLUTION_M / pixel_size_m
                if pixel_size_m < TARGET_RESOLUTION_M
                else 1
            )
            scale_factor = max(scale_by_pixels, scale_by_resolution)
            cyclone_ds = downsample_array(cyclone_raster, scale_factor, method="max")
            friction_ds = downsample_array(friction_arr, scale_factor, method="mean")
            pixel_size_ds = pixel_size_m * scale_factor
        else:
            cyclone_ds = cyclone_raster
            friction_ds = friction_arr
            pixel_size_ds = pixel_size_m

        cost_arr, safe_mask, at_risk_ds = create_cyclone_cost_surface(
            friction_ds, cyclone_ds
        )

        if at_risk_ds.sum() == 0:
            log(f"[{country_code}] No at-risk areas for cyclone, setting nulls")
            df["kt34_evac_time_minutes_mean"] = None
            df["kt34_evac_time_minutes_max"] = None
            df["kt34_evac_time_minutes_median"] = None
        else:
            travel_time_ds = calculate_travel_time_mcp(
                cost_arr, safe_mask, pixel_size_ds
            )

            if scale_factor > 1:
                travel_time = upsample_array(travel_time_ds, original_shape)
                _, _, at_risk_mask = create_cyclone_cost_surface(
                    friction_arr, cyclone_raster
                )
            else:
                travel_time = travel_time_ds
                at_risk_mask = at_risk_ds

            travel_time_at_risk = travel_time.copy()
            travel_time_at_risk[~at_risk_mask] = np.nan
            gdf_tt = gdf.to_crs(raster_crs) if gdf.crs != raster_crs else gdf

            tt_stats = zonal_stats(
                gdf_tt,
                travel_time_at_risk.astype(np.float32),
                affine=cyclone_transform,
                stats=["mean", "max", "median"],
                nodata=np.nan,
            )

            df["kt34_evac_time_minutes_mean"] = [
                round(s["mean"], 1) if s.get("mean") else None for s in tt_stats
            ]
            df["kt34_evac_time_minutes_max"] = [
                round(s["max"], 1) if s.get("max") else None for s in tt_stats
            ]
            df["kt34_evac_time_minutes_median"] = [
                round(s["median"], 1) if s.get("median") else None for s in tt_stats
            ]

        # Validate cyclone evacuability columns
        evac_cols = [
            "kt34_evac_time_minutes_mean",
            "kt34_evac_time_minutes_max",
            "kt34_evac_time_minutes_median",
        ]
        for col in evac_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required cyclone evacuability column: {col}")
            if df[col].isna().all():
                raise ValueError(f"Cyclone evacuability column {col} has no valid data")

        had_data = True
        log(f"[{country_code}] Cyclone evacuability done")

    if not had_data:
        log(f"[{country_code}] No evacuability data produced")
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    log(f"[{country_code}] Evacuability CSV written: {out_csv}")
    return str(out_csv)


def main():
    parser = argparse.ArgumentParser(
        description="Calculate evacuatability indicator: travel time from flooded to safe zones"
    )
    parser.add_argument("country_code", help="ISO3 country code (e.g., STP, PAK, BGD)")
    parser.add_argument(
        "--admin-level", default="ADM2", help="Administrative level (default: ADM2)"
    )
    parser.add_argument("--rp", default="100", help="Return period (default: 100)")
    parser.add_argument(
        "--flood-threshold",
        type=float,
        default=None,
        help=f"Flood depth threshold in meters (default: {FLOOD_THRESHOLD})",
    )
    args = parser.parse_args()
    output_path = process_evacuatability(
        country_code=args.country_code,
        admin_level=args.admin_level,
        rp=args.rp,
        flood_threshold=args.flood_threshold,
    )
    print(f"\nOutput saved to: {output_path}")


if __name__ == "__main__":
    main()
