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
from rasterstats import zonal_stats
import pandas as pd
from shapely.geometry import mapping
import yaml
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
POP_INDICATORS = ["female_pop", "children_u5", "female_u5", "elderly", "pop_u15", "female_u15"]
EXPOSURE_CLASSES = [1, 2, 3]  # cyclone categories

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
        cyclone_raster = src.read(1)
        raster_crs = src.crs

    geojsons_map = {}
    for cat in FACILITY_CATEGORIES:
        geojsons_map[cat] = base_path / f"Temporary/{country_code}_{cat}_raw.geojson"

    df = pd.DataFrame({f"{admin_level}_PCODE": gdf_admin[f"{admin_level}_PCODE"]})
    df["ADM_PCODE"] = df[f"{admin_level}_PCODE"]

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

    # Round all numeric columns to 0 decimal places
    numeric_cols = df.select_dtypes(include=["float", "int"]).columns
    df[numeric_cols] = df[numeric_cols].round(0).astype(int)

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