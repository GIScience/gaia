#!/usr/bin/env python3
"""
process_cyclone_exposure.py

Generates cyclone exposure rasters and computes vulnerable population and facility exposure per admin unit.
Outputs CSV: {country_code}_{admin_level}_cyclone_exposure.csv
"""

import os
from pathlib import Path
import zipfile
import requests
import geopandas as gpd
import numpy as np
import rasterio
from rasterio.features import rasterize
from rasterio.windows import from_bounds, Window
from rasterio.warp import reproject, transform_bounds
from rasterstats import zonal_stats
import pandas as pd
from shapely.geometry import mapping
import yaml
import tempfile
from skimage.graph import MCP_Geometric
from rasterio.enums import Resampling
from scripts.fetch_worldpop import fetch_worldpop
from scripts.fetch_facilities_ohsome_overpass import fetch_overpass, fetch_ohsome

# -----------------------------
# Simple context with info/warning
# -----------------------------
class Context:
    def info(self, msg):
        print(f"INFO: {msg}")

    def warning(self, msg):
        print(f"WARNING: {msg}")

# -----------------------------
# Load asset config
# -----------------------------
ASSET_CONFIG_YAML_PATH = os.path.join(os.getcwd(), "configs", "assets_config.yaml")
with open(ASSET_CONFIG_YAML_PATH) as _fp:
    _asset_config = yaml.safe_load(_fp)

# -----------------------------
# IBTrACS Constants
# -----------------------------
IBTRACS_URL = (
    "https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/"
    "v04r01/access/shapefile/IBTrACS.since1980.list.v04r01.lines.zip"
)
DOWNLOAD_DIR = "downloads"
IBTRACS_LOCAL_ZIP = os.path.join(DOWNLOAD_DIR, "IBTrACS.since1980.list.v04r01.lines.zip")

# -----------------------------
# Config
# -----------------------------
FACILITY_CATEGORIES = ["education", "hospitals", "primary_healthcare"]
POP_INDICATORS = ["total_pop", "female_pop", "children_u5", "female_u5", "elderly", "pop_u15", "female_u15"]
EXPOSURE_CLASSES = [1, 2, 3]  # cyclone categories

FRICTION_COG_URL = "https://hot.storage.heigit.org/heigit-hdx-public/risk_assessment_inputs/2020_motorized_friction_surface_cog.tif"
MAX_PIXELS_FOR_MCP = 10_000_000
TARGET_RESOLUTION_M = 500
MAX_MCP_SOURCES = 20000


# -----------------------------
# Evacuability helper functions
# -----------------------------
def get_pixel_size_meters(transform, crs):
    pixel_x = abs(transform.a)
    pixel_y = abs(transform.e)
    if crs.is_geographic:
        meters_per_degree = 111000
        pixel_size_m = ((pixel_x + pixel_y) / 2) * meters_per_degree
    else:
        pixel_size_m = (pixel_x + pixel_y) / 2
    return pixel_size_m


def downsample_array(arr, scale_factor, method='mean'):
    from scipy.ndimage import zoom
    if scale_factor <= 1:
        return arr
    scale_factor = int(round(scale_factor))
    if method == 'sum':
        h, w = arr.shape
        new_h = (h // scale_factor) * scale_factor
        new_w = (w // scale_factor) * scale_factor
        trimmed = arr[:new_h, :new_w]
        reshaped = trimmed.reshape(new_h // scale_factor, scale_factor, new_w // scale_factor, scale_factor)
        result = np.nansum(reshaped, axis=(1, 3))
        return result.astype(arr.dtype)
    elif method == 'mean':
        return zoom(arr, 1 / scale_factor, order=1)
    elif method == 'max':
        return zoom(arr.astype(float), 1 / scale_factor, order=0) > 0.5
    else:
        return zoom(arr, 1 / scale_factor, order=0)


def upsample_array(arr, target_shape, method='bilinear'):
    from scipy.ndimage import zoom
    scale_y = target_shape[0] / arr.shape[0]
    scale_x = target_shape[1] / arr.shape[1]
    order = 1 if method == 'bilinear' else 0
    return zoom(arr, (scale_y, scale_x), order=order)


def fetch_friction_window(bounds, target_crs, target_transform, target_shape,
                          friction_url=FRICTION_COG_URL):
    bounds_l, bounds_b, bounds_r, bounds_t = bounds
    with rasterio.open(friction_url) as src:
        friction_crs = src.crs
        friction_nodata = src.nodata
        if target_crs != friction_crs:
            src_bounds = transform_bounds(target_crs, friction_crs, bounds_l, bounds_b, bounds_r, bounds_t)
        else:
            src_bounds = (bounds_l, bounds_b, bounds_r, bounds_t)
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


def create_cost_surface(friction_arr, cyclone_arr):
    valid_cyclone = ~np.isnan(cyclone_arr)
    at_risk_mask = valid_cyclone & (cyclone_arr >= 1)
    safe_mask = valid_cyclone & (cyclone_arr == 0)
    cost_arr = friction_arr.copy()
    cost_arr[np.isnan(cost_arr)] = 1e6
    cost_arr[cost_arr <= 0] = 1e6
    return cost_arr, safe_mask, at_risk_mask


def calculate_travel_time_mcp(cost_arr, safe_mask, pixel_size_m):
    safe_indices = np.argwhere(safe_mask)
    if len(safe_indices) == 0:
        return np.full(cost_arr.shape, np.nan)
    cost_scaled = cost_arr * pixel_size_m
    mcp = MCP_Geometric(cost_scaled, fully_connected=True)
    starts = [tuple(idx) for idx in safe_indices]
    if len(starts) > MAX_MCP_SOURCES:
        rng = np.random.default_rng(42)
        indices = rng.choice(len(starts), MAX_MCP_SOURCES, replace=False)
        starts = [starts[i] for i in indices]
    cumulative_cost, traceback = mcp.find_costs(starts)
    travel_time = cumulative_cost.astype(np.float32)
    travel_time[travel_time >= 1e9] = np.nan
    return travel_time

# -----------------------------
# Step 1: IBTrACS download & extract
# -----------------------------
def ensure_ibtracs_data(context: Context):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    if not os.path.exists(IBTRACS_LOCAL_ZIP):
        context.info("Downloading IBTrACS dataset...")
        r = requests.get(IBTRACS_URL)
        r.raise_for_status()
        with open(IBTRACS_LOCAL_ZIP, "wb") as f:
            f.write(r.content)
        context.info("Download complete.")

    extract_path = os.path.join(DOWNLOAD_DIR, "IBTrACS")
    if not os.path.exists(extract_path):
        with zipfile.ZipFile(IBTRACS_LOCAL_ZIP, "r") as zip_ref:
            zip_ref.extractall(extract_path)
            context.info(f"Extracted IBTrACS shapefiles to: {extract_path}")
    return os.path.join(extract_path, "IBTrACS.since1980.list.v04r01.lines.shp")

# -----------------------------
# Step 2: Build cyclone buffers
# -----------------------------
def build_cyclone_buffers(context: Context, country_code: str, admin_level: str):
    shapefile_path = ensure_ibtracs_data(context)
    gdf_ibtracs = gpd.read_file(shapefile_path)
    gdf_ibtracs = gdf_ibtracs[gdf_ibtracs["USA_SSHS"].fillna(0) >= 1]

    boundary_path = f"data/{country_code}/{country_code}_{admin_level}.geojson"
    if not os.path.exists(boundary_path):
        raise FileNotFoundError(f"Boundary file not found: {boundary_path}")
    country_gdf = gpd.read_file(boundary_path)

    bbox = country_gdf.total_bounds
    gdf_ibtracs = gdf_ibtracs.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]]
    if gdf_ibtracs.empty:
        context.info(f"No cyclone tracks near {country_code} bounding box.")
        return None

    gdf_ibtracs = gdf_ibtracs.to_crs(epsg=29738)
    country_gdf = country_gdf.to_crs(epsg=29738)

    gdf_ibtracs["mean_r34"] = gdf_ibtracs[
        ["USA_R34_SE", "USA_R34_NE", "USA_R34_NW", "USA_R34_SW"]
    ].mean(axis=1, skipna=True)
    gdf_ibtracs["mean_r34_m"] = gdf_ibtracs["mean_r34"] * 1852
    gdf_ibtracs["geometry"] = gdf_ibtracs.buffer(gdf_ibtracs["mean_r34_m"].fillna(0))
    
    # Fix potential invalid geometries
    gdf_ibtracs = gdf_ibtracs[~gdf_ibtracs.geometry.is_empty & gdf_ibtracs.geometry.notnull()]
    gdf_ibtracs['geometry'] = gdf_ibtracs.geometry.buffer(0)
    country_gdf['geometry'] = country_gdf.geometry.buffer(0)

    gdf_ibtracs = gpd.clip(gdf_ibtracs, country_gdf)

    out_geojson = f"data/{country_code}/Temporary/{country_code}_cyclone_buffers.geojson"
    os.makedirs(os.path.dirname(out_geojson), exist_ok=True)
    gdf_ibtracs.to_file(out_geojson, driver="GeoJSON")
    context.info(f"Saved cyclone buffer polygons to: {out_geojson}")
    return out_geojson

# -----------------------------
# Step 3: Rasterize buffers
# -----------------------------
def rasterize_cyclone_buffers(context: Context, buffer_geojson: str, country_code: str):
    gdf = gpd.read_file(buffer_geojson)
    if gdf.empty:
        context.info(f"No cyclone buffers found in {buffer_geojson}")
        return None

    temp_dir = Path(f"data/{country_code}/Temporary")
    temp_dir.mkdir(parents=True, exist_ok=True)

    indicator_tifs = fetch_worldpop(country_code)
    reference_tif = indicator_tifs[0]
    with rasterio.open(reference_tif) as src_ref:
        meta = src_ref.meta.copy()
        transform = src_ref.transform
        width = src_ref.width
        height = src_ref.height
        crs = src_ref.crs

    gdf = gdf.to_crs(crs)
    max_raster = np.zeros((height, width), dtype=np.uint8)
    gdf_sorted = gdf.sort_values("USA_SSHS")
    for _, row in gdf_sorted.iterrows():
        if row.geometry is None or np.isnan(row["USA_SSHS"]):
            continue
        level = int(row["USA_SSHS"])
        if not (1 <= level <= 5):
            continue
        shapes = [(row.geometry, level)]
        mask_arr = rasterize(shapes, out_shape=(height, width), transform=transform, fill=0, dtype=np.uint8)
        max_raster = np.maximum(max_raster, mask_arr)

    classified = np.zeros_like(max_raster, dtype=np.uint8)
    classified[(max_raster >= 1) & (max_raster <= 1)] = 1
    classified[(max_raster >= 2) & (max_raster <= 3)] = 2
    classified[(max_raster >= 4) & (max_raster <= 5)] = 3

    out_path = temp_dir / f"{country_code}_cyclone_exposure.tif"
    meta.update(dtype=rasterio.uint8, count=1, compress="lzw")
    with rasterio.open(out_path, "w", **meta) as dst:
        dst.write(classified, 1)

    context.info(f"Classified cyclone raster saved to: {out_path}")
    return str(out_path)

# -----------------------------
# Step 4: Calculate exposure
# -----------------------------
def calculate_cyclone_exposure(context, country_code: str, admin_level="ADM2"):
    country_code = country_code.upper()
    admin_level = admin_level.upper()
    temp_dir = Path(f"data/{country_code}/Temporary")
    temp_dir.mkdir(parents=True, exist_ok=True)
    base_path = Path(f"data/{country_code}")

    buffer_geojson = build_cyclone_buffers(context, country_code, admin_level)
    if not buffer_geojson:
        return None

    raster_path = rasterize_cyclone_buffers(context, buffer_geojson, country_code)
    if not raster_path:
        return None

    boundary_file = base_path / f"{country_code}_{admin_level}.geojson"
    gdf_admin = gpd.read_file(boundary_file).to_crs("EPSG:4326")

    context.info(f"Ensuring demographic rasters exist in {temp_dir}...")
    indicator_tifs = fetch_worldpop(country_code)
    tif_map = dict(zip(POP_INDICATORS, indicator_tifs))

    context.info(f"Ensuring facility raw geometries exist in {temp_dir}...")
    api_choice = _asset_config.get("facilities_asset", {}).get("api", "").lower()
    if api_choice == "ohsome-api":
        fetch_ohsome(context, boundary_file, base_path, country_code, admin_level)
    elif api_choice == "overpass":
        fetch_overpass(context, boundary_file, base_path, country_code, admin_level)
    elif api_choice == "ohsome-parquet":
        context.info("Not implemented yet: ohsome-parquet")
        return None
    else:
        context.warning(f"No valid API configured for facilities_asset (got '{api_choice}')")
        return None

    with rasterio.open(raster_path) as src:
        cyclone_raster = src.read(1).astype(np.float32)
        raster_crs = src.crs
        cyclone_transform = src.transform
        cyclone_bounds = src.bounds
        cyclone_shape = cyclone_raster.shape
        cyclone_nodata = src.nodata

    # Initialize dataframe with admin PCODEs
    df = pd.DataFrame({f"{admin_level}_PCODE": gdf_admin[f"{admin_level}_PCODE"]})
    df["ADM_PCODE"] = df[f"{admin_level}_PCODE"]

    # ---- Evacuatability: travel time to safe zones ----
    context.info("Calculating evacuatability for cyclone exposure...")

    friction_arr = fetch_friction_window(
        bounds=(cyclone_bounds.left, cyclone_bounds.bottom, cyclone_bounds.right, cyclone_bounds.top),
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
        scale_by_resolution = TARGET_RESOLUTION_M / pixel_size_m if pixel_size_m < TARGET_RESOLUTION_M else 1
        scale_factor = max(scale_by_pixels, scale_by_resolution)
        cyclone_raster_ds = downsample_array(cyclone_raster, scale_factor, method='max')
        friction_arr_ds = downsample_array(friction_arr, scale_factor, method='mean')
        pixel_size_m_ds = pixel_size_m * scale_factor
    else:
        cyclone_raster_ds = cyclone_raster
        friction_arr_ds = friction_arr
        pixel_size_m_ds = pixel_size_m

    cost_arr, safe_mask, at_risk_mask_ds = create_cost_surface(friction_arr_ds, cyclone_raster_ds)

    if at_risk_mask_ds.sum() == 0:
        raise ValueError("No at-risk areas found for evacuatability calculation - cannot complete asset")

    travel_time_ds = calculate_travel_time_mcp(cost_arr, safe_mask, pixel_size_m_ds)

    if scale_factor > 1:
        travel_time = upsample_array(travel_time_ds, original_shape)
        at_risk_mask = create_cost_surface(friction_arr, cyclone_raster)[2]
    else:
        travel_time = travel_time_ds
        at_risk_mask = at_risk_mask_ds

    travel_time_at_risk = travel_time.copy()
    travel_time_at_risk[~at_risk_mask] = np.nan

    gdf_tt = gdf_admin.to_crs(raster_crs) if gdf_admin.crs != raster_crs else gdf_admin

    with tempfile.TemporaryDirectory() as tmpdir:
        tt_path = os.path.join(tmpdir, "travel_time.tif")
        with rasterio.open(
            tt_path, 'w',
            driver='GTiff',
            height=travel_time_at_risk.shape[0],
            width=travel_time_at_risk.shape[1],
            count=1,
            dtype=np.float32,
            crs=raster_crs,
            transform=cyclone_transform,
            nodata=np.nan,
        ) as dst:
            dst.write(travel_time_at_risk, 1)

        tt_stats = zonal_stats(
            gdf_tt, tt_path,
            stats=['mean', 'max', 'median', 'count'],
            nodata=np.nan,
        )

        df[f"kt34_evac_time_minutes_mean"] = [round(s['mean'], 1) if s.get('mean') else None for s in tt_stats]
        df[f"kt34_evac_time_minutes_max"] = [round(s['max'], 1) if s.get('max') else None for s in tt_stats]
        df[f"kt34_evac_time_minutes_median"] = [round(s['median'], 1) if s.get('median') else None for s in tt_stats]
        df[f"kt34_pixels_at_risk"] = [s['count'] if s.get('count') else 0 for s in tt_stats]

    # Validate evacuability columns have valid data (not all None)
    evac_cols = ['kt34_evac_time_minutes_mean', 'kt34_evac_time_minutes_max',
                 'kt34_evac_time_minutes_median', 'kt34_pixels_at_risk']
    for col in evac_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required evacuability column: {col}")
        if df[col].isna().all() and col != 'kt34_pixels_at_risk':
            raise ValueError(f"Evacuability column {col} has no valid data")

    context.info("Processed evacuatability for cyclone exposure")

    geojsons_map = {}
    for cat in FACILITY_CATEGORIES:
        geojsons_map[cat] = base_path / f"Temporary/{country_code}_{cat}_raw.geojson"

    # --- Population exposure ---
    for indicator, pop_raster_path in tif_map.items():
        with rasterio.open(pop_raster_path) as src_pop:
            pop_raster = src_pop.read(1)
            meta = src_pop.meta.copy()
        for cls in EXPOSURE_CLASSES:
            mask_cls = (cyclone_raster == cls).astype(np.float32)
            exposed_pop = pop_raster * mask_cls
            temp_path = base_path / f"Temporary/tmp_{indicator}_cat{cls}.tif"
            meta.update(dtype=rasterio.float32, count=1)
            with rasterio.open(temp_path, "w", **meta) as dst:
                dst.write(exposed_pop, 1)
            stats = zonal_stats(gdf_admin, temp_path, stats="sum", nodata=0)
            df[f"kt34_{indicator}_cat{cls}"] = [round(s["sum"] or 0, 0) for s in stats]

    # --- Facility exposure ---
    for category in FACILITY_CATEGORIES:
        filepath = base_path / f"Temporary/{country_code}_{category}_raw.geojson"
        if not filepath.exists():
            continue
        facilities = gpd.read_file(filepath)
        if facilities.empty:
            continue
        facilities = facilities.to_crs(raster_crs)
        facilities["geometry"] = facilities.geometry.centroid
        coords = [(x, y) for x, y in zip(facilities.geometry.x, facilities.geometry.y)]
        with rasterio.open(raster_path) as src:
            values = [v for v in src.sample(coords)]
        facilities["cyclone_class"] = [v[0] for v in values]

        joined = gpd.sjoin(
            facilities,
            gdf_admin[[f"{admin_level}_PCODE", "geometry"]],
            how="inner",
            predicate="within",
        )

        total_facilities = joined.groupby(f"{admin_level}_PCODE").size().to_dict()
        for cls in EXPOSURE_CLASSES:
            mask_cls = joined["cyclone_class"] == cls
            grouped = (
                joined[mask_cls]
                .groupby(f"{admin_level}_PCODE")
                .size()
                .reset_index(name=f"kt34_{category}_count_cat{cls}")
            )
            df = df.merge(grouped, on=f"{admin_level}_PCODE", how="left")
            df[f"kt34_{category}_count_cat{cls}"] = df[f"kt34_{category}_count_cat{cls}"].fillna(0).astype(int)
            # percent
            df[f"kt34_{category}_perc_cat{cls}"] = df.apply(
                lambda x: round((x[f"kt34_{category}_count_cat{cls}"] / total_facilities.get(x[f"{admin_level}_PCODE"], 1)) * 100, 0),
                axis=1,
            )

    # Round all numeric columns to 0 decimal places (except evacuability columns with NaN)
    evac_time_cols = [c for c in df.columns if 'kt34_evac_time' in c]
    numeric_cols = [c for c in df.select_dtypes(include=["float", "int"]).columns if c not in evac_time_cols]
    df[numeric_cols] = df[numeric_cols].fillna(0).round(0).astype(int)

    # Final validation: ensure all required columns exist before saving
    required_cols = ['kt34_evac_time_minutes_mean', 'kt34_evac_time_minutes_max',
                     'kt34_evac_time_minutes_median', 'kt34_pixels_at_risk']
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Asset failed: missing required columns: {missing_cols}")

    # Ensure at least some rows have valid evacuability data
    if df['kt34_pixels_at_risk'].sum() == 0:
        raise ValueError("Asset failed: no pixels at risk found - evacuability calculation produced no valid data")

    output_dir = base_path / "Output"
    output_dir.mkdir(parents=True, exist_ok=True)
    out_csv = output_dir / f"{country_code}_{admin_level}_cyclone_exposure.csv"
    df.to_csv(out_csv, index=False)
    context.info(f"Cyclone exposure CSV saved to: {out_csv}")
    return str(out_csv)

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Process cyclone exposure and vulnerable populations/facilities.")
    parser.add_argument("country_code", help="ISO3 country code, e.g., PHL")
    parser.add_argument("admin_level", nargs="?", default="ADM2", help="Administrative level, default ADM2")
    args = parser.parse_args()

    calculate_cyclone_exposure(args.country_code, args.admin_level)