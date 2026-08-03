#!/usr/bin/env python3
"""
Generate drought exposure CSV using SPEI-6 drought polygons, WorldPop demographics,
and facility data. Produces a CSV with columns per drought class (1-4) for each
demographic indicator and facility category.

Output: data/{country_code}/Output/{country_code}_{admin_level}_drought_exposure.csv
"""

import os
import time
from pathlib import Path
import geopandas as gpd
import numpy as np
import rasterio
from rasterio.features import rasterize
from rasterstats import zonal_stats
import pandas as pd
import yaml

from fetch_worldpop import fetch_worldpop, INDICATORS
from fetch_facilities_ohsome_overpass import fetch_ohsome, fetch_overpass

ASSET_CONFIG_YAML_PATH = os.path.join(os.getcwd(), "configs", "assets_config.yaml")
with open(ASSET_CONFIG_YAML_PATH) as _fp:
    _asset_config = yaml.safe_load(_fp)

DROUGHT_GPKG = os.path.join(
    os.getcwd(), "polygonised_spei_below_15_drought_event_month_share_00501015.gpkg"
)

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

CLASS_LABELS = {
    1: "0-0.05",
    2: "0.05-0.1",
    3: "0.1-0.15",
    4: "0.15+",
}


def rasterize_drought_classes(context, country_code: str, admin_level: str):
    """
    Load drought polygons from the GPKG, rasterize each drought class
    to match the WorldPop reference raster resolution, and return the path
    to the multi-class drought raster.
    """
    country_code = country_code.upper()
    temp_dir = Path(f"data/{country_code}/Temporary")
    temp_dir.mkdir(parents=True, exist_ok=True)

    t_start = time.time()
    drought_gdf = gpd.read_file(DROUGHT_GPKG)
    if drought_gdf.empty:
        context.warning("Drought GPKG is empty.")
        return None
    context.info(
        f"  Loaded GPKG with {len(drought_gdf)} drought classes in {time.time() - t_start:.1f}s"
    )

    indicator_tifs = fetch_worldpop(country_code)
    reference_tif = indicator_tifs[0]
    with rasterio.open(reference_tif) as src_ref:
        meta = src_ref.meta.copy()
        transform = src_ref.transform
        width = src_ref.width
        height = src_ref.height
        crs = src_ref.crs

    drought_gdf = drought_gdf.to_crs(crs)

    class_col = "SPEI-6_below_-1.5_drought_exposure_class"
    if class_col not in drought_gdf.columns:
        context.warning(f"Column '{class_col}' not found in drought GPKG.")
        return None

    classified = np.zeros((height, width), dtype=np.uint8)
    context.info(f"  Raster dimensions: {width}x{height} at {crs}")

    for cls in DROUGHT_CLASSES:
        t_cls = time.time()
        cls_gdf = drought_gdf[drought_gdf[class_col] == cls]
        if cls_gdf.empty:
            context.info(f"  Class {cls} ({CLASS_LABELS[cls]}): no polygons, skipping.")
            continue
        shapes = [(geom, cls) for geom in cls_gdf.geometry]
        mask_arr = rasterize(
            shapes,
            out_shape=(height, width),
            transform=transform,
            fill=0,
            dtype=np.uint8,
        )
        classified = np.maximum(classified, mask_arr)
        n_pixels = int(np.sum(mask_arr > 0))
        context.info(
            f"  Class {cls} ({CLASS_LABELS[cls]}): rasterized in {time.time() - t_cls:.1f}s ({n_pixels} pixels)"
        )

    out_path = temp_dir / f"{country_code}_drought_exposure.tif"
    meta.update(dtype=rasterio.uint8, count=1, compress="lzw")
    with rasterio.open(out_path, "w", **meta) as dst:
        dst.write(classified, 1)

    context.info(f"Drought exposure raster saved to: {out_path}")
    return str(out_path)


def compute_drought_crops(context, country_code, admin_level, gdf, drought_raster_path):
    """
    Compute area of cropland within each drought class per admin unit.
    The drought raster is in EPSG:4326; pixel area is converted from deg² to km²
    using the centroid latitude of the country.
    """
    t_start = time.time()
    temp_dir = Path(f"data/{country_code}/Temporary")
    crop_tifs = sorted(temp_dir.glob(f"{country_code}_crops_*.tif"))
    if not crop_tifs:
        context.info("  No crop rasters found, skipping crop drought exposure.")
        return pd.DataFrame({f"{admin_level}_PCODE": gdf[f"{admin_level}_PCODE"]})

    crop_path = str(crop_tifs[-1])
    context.info(f"Using crop raster: {crop_path}")

    with rasterio.open(drought_raster_path) as src_d:
        drought_raster = src_d.read(1).astype(np.uint8)
        drought_transform = src_d.transform
        drought_crs = src_d.crs
        drought_width = src_d.width
        drought_height = src_d.height

    # Convert pixel area from deg² to km² using the centroid latitude
    centroid_lat = gdf.to_crs("EPSG:4326").union_all().centroid.y
    lat_rad = np.radians(centroid_lat)
    px_deg = abs(drought_transform[0])
    py_deg = abs(drought_transform[4])
    px_km = px_deg * 111.32 * np.cos(lat_rad)
    py_km = py_deg * 111.32
    pixel_area_km2 = px_km * py_km

    # Warp crop raster to match drought raster grid (both EPSG:4326 but possibly different extents)
    context.info(
        f"  Warping crop raster ({drought_width}x{drought_height}) to drought grid..."
    )
    crop_aligned = np.zeros((drought_height, drought_width), dtype=np.float32)
    with rasterio.open(crop_path) as src_c:
        rasterio.warp.reproject(
            source=rasterio.band(src_c, 1),
            destination=crop_aligned,
            src_crs=src_c.crs,
            src_transform=src_c.transform,
            dst_crs=drought_crs,
            dst_transform=drought_transform,
            dst_shape=(drought_height, drought_width),
            resampling=rasterio.enums.Resampling.bilinear,
        )

    df = pd.DataFrame({f"{admin_level}_PCODE": gdf[f"{admin_level}_PCODE"]})

    for cls in DROUGHT_CLASSES:
        mask_cls = (drought_raster == cls).astype(np.float32)
        crop_drought = crop_aligned * mask_cls

        tmp_path = temp_dir / f"tmp_crops_drought_class{cls}.tif"
        meta = {
            "driver": "GTiff",
            "height": drought_height,
            "width": drought_width,
            "count": 1,
            "dtype": rasterio.float32,
            "crs": drought_crs,
            "transform": drought_transform,
            "compress": "lzw",
        }
        with rasterio.open(tmp_path, "w", **meta) as dst:
            dst.write(crop_drought, 1)

        stats = zonal_stats(gdf, tmp_path, stats="sum", nodata=0)
        df[f"spei6_crops_km2_class{cls}"] = [
            round((s["sum"] or 0) * pixel_area_km2, 2) for s in stats
        ]

        os.remove(tmp_path)

    context.info(f"  [{time.time() - t_start:.1f}s] Crop exposure done")
    return df


def calculate_drought_exposure(context, country_code: str, admin_level="ADM2"):
    """
    Main entry point. Loads drought polygons, WorldPop rasters, facilities,
    and computes per-admin-unit drought exposure across 4 drought classes.
    All raster processing stays in EPSG:4326 (matching WorldPop native CRS),
    matching the pattern used by the flood and cyclone exposure assets.
    """
    country_code = country_code.upper()
    admin_level = admin_level.upper()
    temp_dir = Path(f"data/{country_code}/Temporary")
    temp_dir.mkdir(parents=True, exist_ok=True)
    base_path = Path(f"data/{country_code}")

    t_start = time.time()
    context.info("=" * 60)
    context.info(f"Starting drought exposure for {country_code} {admin_level}")
    context.info("=" * 60)

    boundary_file = base_path / f"{country_code}_{admin_level}.geojson"
    if not boundary_file.exists():
        raise FileNotFoundError(f"Boundary file not found: {boundary_file}")

    gdf_admin = gpd.read_file(boundary_file).to_crs("EPSG:4326")
    context.info(
        f"[{time.time() - t_start:.1f}s] Loaded admin boundary with {len(gdf_admin)} units"
    )

    context.info("[1/6] Ensuring demographic rasters exist...")
    t0 = time.time()
    indicator_tifs = fetch_worldpop(country_code)
    full_tif_map = dict(zip(INDICATORS.keys(), indicator_tifs))
    tif_map = {k: full_tif_map[k] for k in POP_INDICATORS}
    context.info(
        f"[{time.time() - t0:.1f}s] WorldPop rasters ready ({len(tif_map)} indicators)"
    )

    context.info("[2/6] Ensuring facility raw geometries exist...")
    t0 = time.time()
    api_choice = _asset_config.get("facilities_asset", {}).get("api", "").lower()
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
    context.info(f"[{time.time() - t0:.1f}s] Facilities ready")

    context.info("[3/6] Rasterizing drought classes to WorldPop resolution...")
    t0 = time.time()
    drought_raster_path = rasterize_drought_classes(context, country_code, admin_level)
    if not drought_raster_path:
        return None
    context.info(f"[{time.time() - t0:.1f}s] Drought raster done")

    with rasterio.open(drought_raster_path) as src:
        drought_raster = src.read(1).astype(np.uint8)
        raster_crs = src.crs

    df = pd.DataFrame({f"{admin_level}_PCODE": gdf_admin[f"{admin_level}_PCODE"]})
    df["ADM_PCODE"] = df[f"{admin_level}_PCODE"]

    context.info(
        f"[4/6] Computing population exposure across {len(POP_INDICATORS)} indicators × {len(DROUGHT_CLASSES)} classes..."
    )
    t0 = time.time()
    for idx, (indicator, pop_raster_path) in enumerate(tif_map.items(), 1):
        t_ind = time.time()
        with rasterio.open(pop_raster_path) as src_pop:
            pop_raster = src_pop.read(1)
            meta = src_pop.meta.copy()
        for cls in DROUGHT_CLASSES:
            mask_cls = (drought_raster == cls).astype(np.float32)
            exposed_pop = pop_raster * mask_cls
            temp_path = base_path / f"Temporary/tmp_{indicator}_drought_class{cls}.tif"
            meta.update(dtype=rasterio.float32, count=1)
            with rasterio.open(temp_path, "w", **meta) as dst:
                dst.write(exposed_pop, 1)
            stats = zonal_stats(gdf_admin, temp_path, stats="sum", nodata=0)
            df[f"spei6_{indicator}_class{cls}"] = [
                round(s["sum"] or 0, 0) for s in stats
            ]
        context.info(
            f"  [{time.time() - t_ind:.1f}s] Indicator {idx}/{len(tif_map)}: {indicator}"
        )
    context.info(f"  Total: [{time.time() - t0:.1f}s]")

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

    context.info(f"[5/6] Computing facility exposure...")
    t0 = time.time()
    for category in FACILITY_CATEGORIES:
        t_cat = time.time()
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
            mask_cls = joined["drought_class"] == cls
            grouped = (
                joined[mask_cls]
                .groupby(f"{admin_level}_PCODE")
                .size()
                .reset_index(name=f"spei6_{category}_count_class{cls}")
            )
            df = df.merge(grouped, on=f"{admin_level}_PCODE", how="left")
            df[f"spei6_{category}_count_class{cls}"] = (
                df[f"spei6_{category}_count_class{cls}"].fillna(0).astype(int)
            )
            df[f"spei6_{category}_perc_class{cls}"] = df.apply(
                lambda x: round(
                    (
                        x[f"spei6_{category}_count_class{cls}"]
                        / total_facilities.get(x[f"{admin_level}_PCODE"], 1)
                    )
                    * 100,
                    0,
                ),
                axis=1,
            )
        context.info(f"  [{time.time() - t_cat:.1f}s] {category}")
    context.info(f"  Total: [{time.time() - t0:.1f}s]")

    context.info(f"[6/6] Computing crop exposure per drought class...")
    t0 = time.time()
    crop_df = compute_drought_crops(
        context, country_code, admin_level, gdf_admin, drought_raster_path
    )
    context.info(f"  [{time.time() - t0:.1f}s] Crop exposure done")
    if f"{admin_level}_PCODE" in crop_df.columns:
        df = df.merge(crop_df, on=f"{admin_level}_PCODE", how="left")

    numeric_cols = [
        c
        for c in df.select_dtypes(include=["float", "int"]).columns
        if "dependency_ratio" not in c
    ]
    df[numeric_cols] = df[numeric_cols].fillna(0).round(0).astype(int)

    output_dir = base_path / "Output"
    output_dir.mkdir(parents=True, exist_ok=True)
    out_csv = output_dir / f"{country_code}_{admin_level}_drought_exposure.csv"
    df.to_csv(out_csv, index=False)
    context.info(
        f"[{time.time() - t_start:.1f}s] DONE — Drought exposure CSV saved to: {out_csv}"
    )
    context.info(f"  Columns: {len(df.columns)}, Rows: {len(df)}")
    return str(out_csv)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Process drought exposure and vulnerable populations/facilities using SPEI-6 data."
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
