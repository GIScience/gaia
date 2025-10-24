import os
import re
import argparse
import requests
import geopandas as gpd
import rasterio
from rasterio.merge import merge
from rasterio.mask import mask
from rasterstats import zonal_stats
from rasterio.enums import Resampling
import pandas as pd
import rioxarray
import yaml
from shapely.geometry import mapping
from pathlib import Path

from scripts.fetch_worldpop import fetch_worldpop
from scripts.fetch_facilities_ohsome_overpass import fetch_overpass, fetch_ohsome

ASSET_CONFIG_YAML_PATH = os.path.join(os.getcwd(), "configs", "assets_config.yaml")
with open(ASSET_CONFIG_YAML_PATH) as _fp:
    _asset_config = yaml.safe_load(_fp)

BASE_URL_TEMPLATE = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/CEMS-GLOFAS/flood_hazard/{rp}/"
ALLOWED_RPS = _asset_config.get("setup", {}).get("rps", [])

try:
    FLOOD_THRESHOLD = float(_asset_config["setup"]["flood_threshold"])
except KeyError:
    raise KeyError("Missing 'setup.flood_threshold' in assets_config.yaml")

THRESH_SUFFIX = f"{int(FLOOD_THRESHOLD*100)}cm"


def parse_listing(rp):
    url = BASE_URL_TEMPLATE.format(rp=f"RP{rp}")
    r = requests.get(url)
    r.raise_for_status()
    return re.findall(r'href="([^"]+_RP{}_depth\.tif)"'.format(rp), r.text)


def tile_bounds_from_filename(fname):
    m = re.search(r'_(N|S)(\d+)_([EW])(\d+)_RP', fname)
    if not m:
        return None
    lat_sign = 1 if m.group(1) == "N" else -1
    lat = int(m.group(2)) * lat_sign
    lon_sign = 1 if m.group(3) == "E" else -1
    lon = int(m.group(4)) * lon_sign
    xmin = lon
    xmax = lon + 10
    ymin = lat - 10
    ymax = lat
    return (xmin, ymin, xmax, ymax)


def bbox_intersects(tile_bbox, geom_bbox):
    txmin, tymin, txmax, tymax = tile_bbox
    gxmin, gymin, gxmax, gymax = geom_bbox
    return not (txmax <= gxmin or txmin >= gxmax or tymax <= gymin or tymin >= gymax)


def download_file(context, fname, temporary_dir, rp):
    url = BASE_URL_TEMPLATE.format(rp=f"RP{rp}") + fname
    outpath = os.path.join(temporary_dir, fname)
    if os.path.exists(outpath):
        context.info(f"Already exists: {fname}")
        return outpath
    context.info(f"Downloading {fname}...")
    r = requests.get(url, stream=True)
    if r.status_code == 200:
        with open(outpath, "wb") as f:
            for chunk in r.iter_content(1024 * 64):
                f.write(chunk)
        return outpath
    else:
        context.warning(f"Failed to download {fname}")
        return None


def process_country_rp(context, country_code, rp, admin_level="ADM0"):
    temporary_dir = f"data/{country_code}/Temporary"
    output_dir = f"data/{country_code}/Output"
    os.makedirs(temporary_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    clipped_path = os.path.join(temporary_dir, f"{country_code}_flooded_RP{rp}.tif")
    if os.path.exists(clipped_path):
        context.info(f"Clipped raster already exists: {clipped_path}, skipping ...")
        return clipped_path

    boundary_file = os.path.join("data", country_code, f"{country_code}_{admin_level}.geojson")
    if not os.path.exists(boundary_file):
        raise FileNotFoundError(f"Boundary file not found: {boundary_file}")

    gdf = gpd.read_file(boundary_file)
    gxmin, gymin, gxmax, gymax = gdf.total_bounds
    geom_bbox = (gxmin, gymin, gxmax, gymax)
    context.info(f"Boundary BBOX for {country_code}: {geom_bbox}")

    files = parse_listing(rp)
    context.info(f"Found {len(files)} tiles on server for RP{rp}.")

    selected = [f for f in files if (tb := tile_bounds_from_filename(f)) and bbox_intersects(tb, geom_bbox)]
    context.info(f"Selected {len(selected)} tiles to download.")

    tile_paths = [download_file(context, f, temporary_dir, rp) for f in selected]
    tile_paths = [p for p in tile_paths if p]

    if not tile_paths:
        context.warning(f"No tiles downloaded for RP{rp}, skipping.")
        return None

    context.info("Merging tiles...")
    srcs = [rasterio.open(p) for p in tile_paths]
    mosaic, out_trans = merge(srcs)

    out_meta = srcs[0].meta.copy()
    out_meta.update({
        "driver": "GTiff",
        "height": mosaic.shape[1],
        "width": mosaic.shape[2],
        "transform": out_trans,
        "compress": "lzw"
    })

    context.info("Clipping merged raster to boundary...")
    if gdf.crs != srcs[0].crs:
        gdf = gdf.to_crs(srcs[0].crs)

    geoms = [mapping(geom) for geom in gdf.geometry]
    with rasterio.io.MemoryFile() as memfile:
        with memfile.open(**out_meta) as dataset:
            dataset.write(mosaic)
            out_image, out_transform = mask(dataset, geoms, crop=True)

    out_meta.update({
        "height": out_image.shape[1],
        "width": out_image.shape[2],
        "transform": out_transform
    })

    with rasterio.open(clipped_path, "w", **out_meta) as dest:
        dest.write(out_image)

    context.info(f"Clipped raster saved to {clipped_path}")

    for src in srcs:
        src.close()

    return clipped_path


def process_flood_impact(context, country_code, rps, gdf, admin_level, output_dir):
    """
    Process flooded population for all RPs of a given country/admin_level.
    Generates a single CSV with columns for each RP, indicator, threshold,
    and percentage of facilities flooded.
    """
    country_code = country_code.upper()
    output_dir = Path(output_dir)
    out_csv = output_dir / f"{country_code}_{admin_level}_flood_exposure.csv"
    temp_dir = Path("data") / country_code / "Temporary"

    # Load existing CSV if present, to append missing RPs
    if out_csv.exists():
        context.info(f"CSV exists: {out_csv}, will append missing RPs")
        final_df = pd.read_csv(out_csv)
    else:
        final_df = pd.DataFrame({f"{admin_level}_PCODE": gdf[f"{admin_level}_PCODE"]})

    # Ensure WorldPop files exist
    context.info(f"Ensuring demographic rasters exist in {temp_dir}...")
    indicator_tifs = fetch_worldpop(country_code)
    indicators = ["female_pop", "children_u5", "female_u5", "elderly", "pop_u15", "female_u15"]
    tif_map = dict(zip(indicators, indicator_tifs))

    # Ensure facilities exist
    context.info(f"Ensuring facility raw geometries exist in {temp_dir}...")
    base_path = Path("data") / country_code
    boundary_path = base_path / f"{country_code}_{admin_level}.geojson"
    api_choice = _asset_config.get("facilities_asset", {}).get("api", "").lower()

    if api_choice == "ohsome-api":
        summary_path = fetch_ohsome(
            context, boundary_path, base_path, country_code, admin_level
        )
    elif api_choice == "overpass":
        summary_path = fetch_overpass(
            context, boundary_path, base_path, country_code, admin_level
        )
    elif api_choice == "ohsome-parquet":
        context.info("Not implemented yet: ohsome-parquet")
        return None
    else:
        context.warning(
            f"No valid API configured for facilities_asset (got '{api_choice}')"
        )
        return None

    geojsons_map = {}
    facility_categories = ["education", "hospitals", "primary_healthcare"]
    for category in facility_categories:
        if category not in geojsons_map:
            geojsons_map[category] = base_path / f"Temporary/{country_code}_{category}_raw.geojson"

    for rp in rps:
        context.info(f"Processing RP{rp}...")

        # Skip RP if all expected columns already exist
        expected_cols = [
            f"RP{rp}_{label}_{suffix}" for label in indicators for suffix in THRESH_SUFFIX
        ] + [
            f"RP{rp}_{cat}_{suffix}_pct" for cat in facility_categories for suffix in THRESH_SUFFIX
        ] + [
            f"RP{rp}_{cat}_{suffix}_count" for cat in facility_categories for suffix in THRESH_SUFFIX
        ]
        if all(col in final_df.columns for col in expected_cols):
            context.info(f"RP{rp} already processed, skipping...")
            continue

        # Flood raster clipped to country
        clipped_path = process_country_rp(context, country_code, rp, admin_level)
        if not clipped_path:
            context.warning(f"No flood raster for RP{rp}, skipping...")
            continue
        flood = rioxarray.open_rasterio(clipped_path, masked=True).squeeze()

        rp_df = pd.DataFrame({f"{admin_level}_PCODE": gdf[f"{admin_level}_PCODE"]})

        # ---- Flooded population ----
        for label in indicators:
            pop_raster_path = tif_map[label]
            pop_raster = rioxarray.open_rasterio(pop_raster_path, masked=True).squeeze()

            flood_aligned = flood.rio.reproject_match(pop_raster, resampling=Resampling.bilinear)
            flood_mask = (flood_aligned > FLOOD_THRESHOLD).astype("float32")
            flooded_pop = pop_raster * flood_mask

            tmp_flooded = temp_dir / f"tmp_flooded_{label}_RP{rp}_{THRESH_SUFFIX}.tif"
            with rasterio.open(pop_raster_path) as src:
                meta = src.meta.copy()
                meta.update(compress="lzw", tiled=True,
                            bigtiff="yes" if src.width*src.height > 2**32 else "no")
            with rasterio.open(tmp_flooded, "w", **meta) as dst:
                dst.write(flooded_pop.values, 1)

            stats = zonal_stats(gdf, tmp_flooded, stats="sum", nodata=0)
            rp_df[f"RP{rp}_{label}_{THRESH_SUFFIX}"] = [s["sum"] if s["sum"] is not None else 0 for s in stats]

            context.info(f"Processed flooded population for {label} >{FLOOD_THRESHOLD} m ({THRESH_SUFFIX})")

        # ---- Flooded facilities ----
        for category, filepath in geojsons_map.items():
            if not Path(filepath).exists():
                rp_df[f"RP{rp}_{category}_{THRESH_SUFFIX}_pct"] = 0
                rp_df[f"RP{rp}_{category}_{THRESH_SUFFIX}_count"] = 0
                continue

            facilities = gpd.read_file(filepath)
            if facilities.empty:
                rp_df[f"RP{rp}_{category}_{THRESH_SUFFIX}_pct"] = 0
                rp_df[f"RP{rp}_{category}_{THRESH_SUFFIX}_count"] = 0
                continue

            if not all(facilities.geometry.type == "Point"):
                if facilities.crs != flood.rio.crs:
                    facilities = facilities.to_crs(flood.rio.crs)
                facilities["geometry"] = facilities.geometry.centroid
            if facilities.crs != flood.rio.crs:
                facilities = facilities.to_crs(flood.rio.crs)

            coords = [(x, y) for x, y in zip(facilities.geometry.x, facilities.geometry.y)]
            with rasterio.open(clipped_path) as src:
                values = [v[0] for v in src.sample(coords)]
            facilities["flooded"] = [1 if v > FLOOD_THRESHOLD else 0 for v in values]

            joined = gpd.sjoin(
                facilities, gdf[[f"{admin_level}_PCODE", "geometry"]],
                how="inner", predicate="within"
            )
            grouped = joined.groupby(f"{admin_level}_PCODE")["flooded"].agg(["mean", "sum"]).reset_index()

            grouped[f"RP{rp}_{category}_{THRESH_SUFFIX}_pct"] = (grouped["mean"] * 100).round(1)
            grouped[f"RP{rp}_{category}_{THRESH_SUFFIX}_count"] = grouped["sum"].astype(int)
            grouped = grouped.drop(columns=["mean", "sum"])

            rp_df = rp_df.merge(grouped, on=f"{admin_level}_PCODE", how="left")
            rp_df[f"RP{rp}_{category}_{THRESH_SUFFIX}_pct"] = rp_df[f"RP{rp}_{category}_{THRESH_SUFFIX}_pct"].fillna(0)
            rp_df[f"RP{rp}_{category}_{THRESH_SUFFIX}_count"] = rp_df[f"RP{rp}_{category}_{THRESH_SUFFIX}_count"].fillna(0).astype(int)

            context.info(f"Processed flooded facilities for {category} >{FLOOD_THRESHOLD} m ({THRESH_SUFFIX})")

        existing_cols = set(final_df.columns)
        rp_df = rp_df[[c for c in rp_df.columns if c not in existing_cols or c == f"{admin_level}_PCODE"]]
        final_df = final_df.merge(rp_df, on=f"{admin_level}_PCODE", how="left")
        context.info(f"Processed RP{rp}")

    numeric_cols = final_df.select_dtypes(include=["float", "int"]).columns
    final_df[numeric_cols] = final_df[numeric_cols].fillna(0).round(0).astype(int)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Add ADM_PCODE duplicate for schema consistency
    admin_col = f"{admin_level}_PCODE"
    if "ADM_PCODE" not in final_df.columns and admin_col in final_df.columns:
        final_df["ADM_PCODE"] = final_df[admin_col]

    # Reorder columns so ADM_PCODE follows the main admin column
    cols = [admin_col, "ADM_PCODE"] + [c for c in final_df.columns if c not in [admin_col, "ADM_PCODE"]]
    final_df = final_df[cols]

    output_dir.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(out_csv, index=False)
    context.info(f"Flooded population & facilities CSV written to {out_csv}")

    return str(out_csv)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download, merge, clip GLOFAS flood hazard and overlay with WorldPop demographics."
    )
    parser.add_argument("country_code", help="ISO3 country code (e.g. RWA, KEN, BGD)")
    parser.add_argument(
        "admin_level",
        nargs="?",
        default="ADM0",
        help="Administrative level (ADM0, ADM1, ADM2, etc.). Default is ADM0"
    )
    parser.add_argument(
        "--rp",
        help=f"Return period (allowed: {', '.join(ALLOWED_RPS)}). If omitted, all allowed RPs are processed.",
        default=None
    )
    args = parser.parse_args()

    country_code = args.country_code.upper()
    admin_level = args.admin_level.upper()

    rps_to_process = [args.rp.strip()] if args.rp else ALLOWED_RPS
    for rp in rps_to_process:
        if rp not in ALLOWED_RPS:
            raise ValueError(f"Invalid RP '{rp}'. Allowed values: {', '.join(ALLOWED_RPS)}")

        print(f"\n=== Processing {country_code}, {admin_level}, RP{rp} ===")

        clipped_path = process_country_rp(country_code, rp, admin_level)

        temporary_dir = f"data/{country_code}/Temporary"
        output_dir = f"data/{country_code}/Output"
        gdf_path = os.path.join("data", country_code, f"{country_code}_{admin_level}.geojson")
        if not os.path.exists(gdf_path):
            raise FileNotFoundError(f"Boundary file not found: {gdf_path}")
        gdf = gpd.read_file(gdf_path)

        process_flood_impact(
            country_code=country_code,
            rps=[rp],
            gdf=gdf,
            admin_level=admin_level,
            output_dir=output_dir
        )