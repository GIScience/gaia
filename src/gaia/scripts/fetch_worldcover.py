#!/usr/bin/env python3
"""
Builds a per-country cropland-fraction raster from ESA WorldCover 10 m land
cover, aligned to that country's WorldPop reference grid, and computes
exposed cropland area (km²) for flood, cyclone, and drought exposure — the
same way population exposure is computed for each of those hazards.

Data source: https://esa-worldcover.org (public S3 bucket, unauthenticated).
Local cache: <repo_root>/downloads/worldcover/tiles/ — shared across
countries, since neighboring countries often share border tiles.
Output: data/{country_code}/Temporary/{country_code}_crops_{year}.tif —
fractional cropland cover (0-1) per WorldPop pixel.

ESA WorldCover ships exactly two map releases (2020, 2021) rather than an
annual product, so a requested year snaps to the closest available release.
"""

from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.io import MemoryFile
from rasterio.warp import reproject
from rasterstats import zonal_stats

# Re-exported so existing `from gaia.scripts.fetch_worldcover import
# DEFAULT_CROPS_YEAR` call sites (drought, cyclone, flood) keep working —
# constants.py is the single source of truth for the actual value.
from gaia.defs.constants import DEFAULT_CROPS_YEAR, REPO_ROOT  # noqa: F401
from gaia.scripts.download_utils import download_file

WORLDCOVER_DIR = REPO_ROOT / "downloads" / "worldcover"
WORLDCOVER_TILES_DIR = WORLDCOVER_DIR / "tiles"

S3_URL_PREFIX = "https://esa-worldcover.s3.eu-central-1.amazonaws.com"
GRID_URL = f"{S3_URL_PREFIX}/esa_worldcover_grid.geojson"
GRID_PATH = WORLDCOVER_DIR / "esa_worldcover_grid.geojson"

WORLDCOVER_VERSIONS = {2020: "v100", 2021: "v200"}
CROPLAND_CLASS = 40  # ESA WorldCover legend code for "Cropland"


def _resolve_year(year: int) -> int:
    return year if year in WORLDCOVER_VERSIONS else DEFAULT_CROPS_YEAR


def _load_tile_grid(context) -> gpd.GeoDataFrame:
    WORLDCOVER_DIR.mkdir(parents=True, exist_ok=True)
    if not GRID_PATH.exists():
        context.info("Downloading ESA WorldCover tile grid...")
        download_file(GRID_URL, str(GRID_PATH))
    return gpd.read_file(GRID_PATH)


def _tiles_for_boundary(context, boundary_gdf: gpd.GeoDataFrame) -> list[str]:
    grid = _load_tile_grid(context)
    gdf = boundary_gdf.to_crs(grid.crs) if boundary_gdf.crs != grid.crs else boundary_gdf
    geom = gdf.union_all()
    tiles = grid[grid.intersects(geom)]
    return tiles["ll_tile"].tolist()


def _ensure_tile(context, tile: str, year: int) -> Path | None:
    version = WORLDCOVER_VERSIONS[year]
    fname = f"ESA_WorldCover_10m_{year}_{version}_{tile}_Map.tif"
    dest = WORLDCOVER_TILES_DIR / fname
    if not dest.exists():
        WORLDCOVER_TILES_DIR.mkdir(parents=True, exist_ok=True)
        url = f"{S3_URL_PREFIX}/{version}/{year}/map/{fname}"
        context.info(f"Downloading ESA WorldCover tile: {fname} ...")
        # Not every grid cell has a corresponding file (ocean-only tiles) —
        # soft=True skips those instead of failing the whole build.
        result = download_file(url, str(dest), soft=True)
        if result is None:
            return None
    return dest


def fetch_cropland_fraction(
    context, country_code: str, year: int = DEFAULT_CROPS_YEAR
) -> str | None:
    """
    Build (or reuse a cached) fractional-cropland-cover raster for
    `country_code`, aligned to that country's WorldPop reference grid. Each
    output pixel is the fraction (0-1) of its area classified as ESA
    WorldCover cropland, computed by averaging the native 10 m
    classification down to the WorldPop grid.

    Tiles are processed and reprojected one at a time (never merged in
    memory), so peak memory stays bounded to a single decompressed 10 m
    tile (~1.3 GB) regardless of how many tiles a country spans.
    """
    from gaia.scripts.fetch_worldpop import fetch_worldpop

    year = _resolve_year(year)
    country_code = country_code.upper()
    temp_dir = Path(f"data/{country_code}/Temporary")
    temp_dir.mkdir(parents=True, exist_ok=True)
    out_path = temp_dir / f"{country_code}_crops_{year}.tif"
    if out_path.exists():
        context.info(f"Cropland fraction raster already exists: {out_path}")
        return str(out_path)

    boundary_file = Path(f"data/{country_code}/{country_code}_ADM0.geojson")
    if not boundary_file.exists():
        context.warning(
            f"No ADM0 boundary found for {country_code}; skipping cropland raster."
        )
        return None
    boundary_gdf = gpd.read_file(boundary_file)

    tiles = _tiles_for_boundary(context, boundary_gdf)
    if not tiles:
        context.warning(f"No ESA WorldCover tiles intersect {country_code}.")
        return None

    indicator_tifs = fetch_worldpop(country_code)
    with rasterio.open(indicator_tifs[0]) as ref:
        dst_transform = ref.transform
        dst_crs = ref.crs
        dst_height, dst_width = ref.height, ref.width
        meta = ref.meta.copy()

    context.info(
        f"Building cropland fraction raster for {country_code} from "
        f"{len(tiles)} ESA WorldCover tile(s) (year {year})..."
    )
    fraction = np.zeros((dst_height, dst_width), dtype=np.float32)
    used_tiles = 0
    for tile in tiles:
        tile_path = _ensure_tile(context, tile, year)
        if tile_path is None:
            continue

        with rasterio.open(tile_path) as src:
            cropland = (src.read(1) == CROPLAND_CLASS).astype(np.uint8)
            src_transform = src.transform
            src_crs = src.crs

        with MemoryFile() as memfile:
            with memfile.open(
                driver="GTiff",
                height=cropland.shape[0],
                width=cropland.shape[1],
                count=1,
                dtype="uint8",
                crs=src_crs,
                transform=src_transform,
            ) as mem_ds:
                mem_ds.write(cropland, 1)
                # init_dest_nodata=False: only pixels covered by this tile's
                # footprint are written, so `fraction` values from earlier
                # (non-overlapping) tiles are preserved.
                reproject(
                    source=rasterio.band(mem_ds, 1),
                    destination=fraction,
                    src_transform=src_transform,
                    src_crs=src_crs,
                    dst_transform=dst_transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.average,
                    init_dest_nodata=False,
                )
        used_tiles += 1
        del cropland

    if used_tiles == 0:
        context.warning(
            f"No ESA WorldCover tiles could be downloaded for {country_code}."
        )
        return None

    meta.update(dtype="float32", count=1, compress="lzw", nodata=None)
    with rasterio.open(out_path, "w", **meta) as dst:
        dst.write(fraction, 1)

    context.info(f"Cropland fraction raster saved to: {out_path}")
    return str(out_path)


def crop_exposure_km2(
    context,
    country_code: str,
    admin_gdf: gpd.GeoDataFrame,
    masks: dict,
    year: int = DEFAULT_CROPS_YEAR,
):
    """
    Compute exposed cropland area (km²) per admin unit for one or more
    hazard masks.

    `masks` maps a label to a 2D array already aligned to the country's
    WorldPop reference grid (the same grid every hazard's own exposure mask
    is already computed on) — e.g. a boolean flood mask, or a per-class
    drought/cyclone mask. Returns {label: [km2 per admin_gdf row]}, or None
    if no cropland raster could be built for this country (e.g. no
    WorldCover tiles available).
    """
    cropland_path = fetch_cropland_fraction(context, country_code, year)
    if cropland_path is None:
        return None

    with rasterio.open(cropland_path) as src:
        cropland_fraction = src.read(1)
        transform = src.transform

    # Pixel area in km², accounting for latitude-dependent degree size
    # (matches the approximation used elsewhere in this pipeline for
    # geographic-CRS pixel-area conversion).
    centroid_lat = admin_gdf.to_crs("EPSG:4326").union_all().centroid.y
    lat_rad = np.radians(centroid_lat)
    pixel_area_km2 = (
        abs(transform.a) * 111.32 * np.cos(lat_rad) * abs(transform.e) * 111.32
    )

    result = {}
    for label, mask_arr in masks.items():
        exposed = (cropland_fraction * np.asarray(mask_arr)).astype(np.float32)
        stats = zonal_stats(admin_gdf, exposed, affine=transform, stats="sum", nodata=0)
        result[label] = [round((s["sum"] or 0) * pixel_area_km2, 2) for s in stats]
    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Build the ESA WorldCover cropland-fraction raster for a country."
    )
    parser.add_argument("country_code", help="ISO3 country code, e.g., MMR")
    parser.add_argument("--year", type=int, default=DEFAULT_CROPS_YEAR)
    args = parser.parse_args()

    class PrintLogger:
        def info(self, msg):
            print(f"INFO: {msg}")

        def warning(self, msg):
            print(f"WARNING: {msg}")

    fetch_cropland_fraction(PrintLogger(), args.country_code.upper(), args.year)
