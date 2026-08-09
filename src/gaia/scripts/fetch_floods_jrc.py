import os
import re
import logging
import argparse
import tempfile
import requests
import geopandas as gpd
import numpy as np
import rasterio
from rasterio.merge import merge
from rasterio.mask import mask
from rasterio.warp import reproject
from rasterstats import zonal_stats
from rasterio.enums import Resampling
import pandas as pd
import rioxarray
from shapely.geometry import mapping
from pathlib import Path

from gaia.defs.utils import estimate_raster_cells, to_4326
from gaia.scripts.fetch_worldpop import fetch_worldpop, INDICATORS
from gaia.scripts.fetch_facilities_ohsome_overpass import fetch_overpass, fetch_ohsome

BASE_URL_TEMPLATE = (
    "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/CEMS-GLOFAS/flood_hazard/{rp}/"
)
ALLOWED_RPS = ["10", "50", "100", "500"]


def parse_listing(rp):
    url = BASE_URL_TEMPLATE.format(rp=f"RP{rp}")
    r = requests.get(url)
    r.raise_for_status()
    return re.findall(r'href="([^"]+_RP{}_depth\.tif)"'.format(rp), r.text)


def tile_bounds_from_filename(fname):
    m = re.search(r"_(N|S)(\d+)_([EW])(\d+)_RP", fname)
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
    from gaia.scripts.download_utils import download_file as _download_file

    url = BASE_URL_TEMPLATE.format(rp=f"RP{rp}") + fname
    outpath = os.path.join(temporary_dir, fname)
    if os.path.exists(outpath):
        context.info(f"Already exists: {fname}")
        return outpath
    context.info(f"Downloading {fname}...")
    path = _download_file(url, outpath, soft=True)
    if path is None:
        context.warning(f"Failed to download {fname}")
        return None
    return path


def _tile_paths_for_bbox(context, country_code, rp, geom_bbox):
    """Select GLOFAS tiles intersecting the given bounding box and return their
    local paths (downloading any missing ones into Temporary/)."""
    temporary_dir = f"data/{country_code}/Temporary"
    files = parse_listing(rp)
    context.info(f"Found {len(files)} tiles on server for RP{rp}.")

    selected = [
        f
        for f in files
        if (tb := tile_bounds_from_filename(f)) and bbox_intersects(tb, geom_bbox)
    ]
    context.info(f"Selected {len(selected)} tiles to download.")

    tile_paths = [download_file(context, f, temporary_dir, rp) for f in selected]
    return [p for p in tile_paths if p]


def _merge_clipped_flood(context, country_code, rp, gdf, clipped_path):
    """Download the GLOFAS tiles intersecting `gdf`'s bbox, clip them to the
    geometry and merge, then write the mosaic to `clipped_path`.

    Both the per-tile clip and the merge stream to disk, so peak memory stays
    bounded by a single clipped tile instead of the whole country mosaic
    (which used to OOM for large countries like Brazil)."""
    gxmin, gymin, gxmax, gymax = gdf.total_bounds
    geom_bbox = (gxmin, gymin, gxmax, gymax)
    context.info(f"Boundary BBOX for {country_code}: {geom_bbox}")

    tile_paths = _tile_paths_for_bbox(context, country_code, rp, geom_bbox)

    if not tile_paths:
        context.warning(f"No tiles downloaded for RP{rp}, skipping.")
        return None

    context.info("Clipping tiles to boundary...")
    srcs = [rasterio.open(p) for p in tile_paths]

    if gdf.crs != srcs[0].crs:
        gdf = gdf.to_crs(srcs[0].crs)

    geoms = [mapping(geom) for geom in gdf.geometry]
    base_meta = srcs[0].meta.copy()

    # Clip each tile to the boundary and stage the pieces on disk rather than
    # in MemoryFiles: the pieces together equal the full country mosaic, which
    # is multi-GB for large countries. Peak RAM is then just one clip window.
    piece_paths = []
    for i, src in enumerate(srcs):
        try:
            out_image, out_transform = mask(src, geoms, crop=True, filled=True)
        except (rasterio.errors.WindowError, ValueError):
            src.close()
            continue
        if out_image.shape[1] == 0 or out_image.shape[2] == 0:
            src.close()
            continue

        meta = base_meta.copy()
        meta.update(
            {
                "height": out_image.shape[1],
                "width": out_image.shape[2],
                "transform": out_transform,
                "compress": "lzw",
            }
        )

        fd, piece_path = tempfile.mkstemp(
            suffix=".tif", prefix=f"{country_code}_RP{rp}_clip_"
        )
        os.close(fd)
        with rasterio.open(piece_path, "w", **meta) as dataset:
            dataset.write(out_image)
        piece_paths.append(piece_path)
        src.close()

    if not piece_paths:
        context.warning(f"No tiles overlap the {country_code} boundary, skipping.")
        return None

    try:
        context.info("Merging clipped tiles (streaming to disk)...")
        pieces = [rasterio.open(p) for p in piece_paths]
        try:
            merge(
                pieces,
                dst_path=clipped_path,
                dst_kwds={"driver": "GTiff", "compress": "lzw"},
            )
        finally:
            for ds in pieces:
                ds.close()
    finally:
        for p in piece_paths:
            try:
                os.remove(p)
            except OSError:
                pass

    context.info(f"Clipped raster saved to {clipped_path}")

    return clipped_path


def _build_flood_mask_1km(
    context, country_code, rp, clipped_path, mask_path, ref_pop_path
):
    """Reproject the clipped flood mosaic onto the WorldPop 1 km grid,
    streaming window-by-window so peak memory stays bounded, and write the
    result to `mask_path`.

    Every spatial chunk (and the whole-country run) reads this single file, so
    per-unit population sums are identical regardless of chunking. (A per-chunk
    reprojection would not be: GDAL's bilinear warp is sensitive to the source
    raster extent, so identical source pixels yield slightly different values
    on a chunk raster than on the full-country raster.)"""
    with rasterio.open(ref_pop_path) as ref:
        dst_transform = ref.transform
        dst_crs = ref.crs
        dst_height, dst_width = ref.height, ref.width

    with (
        rasterio.open(clipped_path) as src,
        rasterio.open(
            mask_path,
            "w",
            driver="GTiff",
            height=dst_height,
            width=dst_width,
            count=1,
            dtype="float32",
            crs=dst_crs,
            transform=dst_transform,
            compress="lzw",
            nodata=np.nan,
        ) as dst,
    ):
        reproject(
            source=rasterio.band(src, 1),
            destination=rasterio.band(dst, 1),
            src_transform=src.transform,
            src_crs=src.crs,
            src_nodata=src.nodata,
            dst_transform=dst_transform,
            dst_crs=dst_crs,
            dst_nodata=np.nan,
            resampling=Resampling.bilinear,
        )

    context.info(f"Flood 1 km mask saved to {mask_path}")

    return mask_path


def process_country_rp(context, country_code, rp, admin_level="ADM0"):
    temporary_dir = f"data/{country_code}/Temporary"
    output_dir = f"data/{country_code}/Output"
    os.makedirs(temporary_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    clipped_path = os.path.join(temporary_dir, f"{country_code}_flooded_RP{rp}.tif")
    if os.path.exists(clipped_path):
        context.info(f"Clipped raster already exists: {clipped_path}, skipping ...")
        return clipped_path

    boundary_file = os.path.join(
        "data", country_code, f"{country_code}_{admin_level}.geojson"
    )
    if not os.path.exists(boundary_file):
        raise FileNotFoundError(f"Boundary file not found: {boundary_file}")

    gdf = to_4326(gpd.read_file(boundary_file))

    return _merge_clipped_flood(context, country_code, rp, gdf, clipped_path)


def _split_into_chunks(gdf, res_deg, chunk_max_cells):
    """Split gdf into row-contiguous groups of admin units so each group's
    raster footprint (bbox cells at resolution `res_deg`) stays within
    `chunk_max_cells`. A single unit that alone exceeds the limit becomes its
    own chunk (it cannot be split further)."""
    chunks = []
    start = 0
    while start < len(gdf):
        end = start
        while end < len(gdf):
            sub = gdf.iloc[start : end + 1]
            if estimate_raster_cells(sub, res_deg) > chunk_max_cells and end > start:
                break
            end += 1
        chunks.append(gdf.iloc[start:end])
        start = end
    return chunks


def _compute_rp_exposure(
    context,
    country_code,
    rp,
    unit_gdf,
    admin_level,
    temp_dir,
    clipped_path,
    flood_threshold,
    thresh_suffix,
    tif_map,
    indicators,
    geojsons_map,
    crop_years,
    ee_initialized,
    chunk_tag="",
    chunk_label="",
    flood_mask_path=None,
):
    """Compute flooded crops/population/facilities for a single RP over a
    single slice of admin units (the whole country or one spatial chunk).
    Returns a per-unit rp_df. `chunk_tag` disambiguates temporary crop CSVs
    when several spatial chunks share the same RP.

    `flood_mask_path` is the shared country-wide 1 km flood mask built by
    `_build_flood_mask_1km`. When provided, the population overlay reads that
    single file (identical for every chunk and for the whole-country run)
    instead of reprojecting the 100 m mosaic per call."""
    rp_df = pd.DataFrame({f"{admin_level}_PCODE": unit_gdf[f"{admin_level}_PCODE"]})

    # ---- Flooded crops (GEE pixel-level overlay via JRC GLOFAS + Dynamic World) ----
    if crop_years:
        rp_df[f"RP{rp}_crops_{thresh_suffix}_km2"] = 0.0
        rp_df[f"RP{rp}_crops_{thresh_suffix}_areapct"] = 0.0
        rp_df[f"RP{rp}_crops_{thresh_suffix}_croppct"] = 0.0

        if ee_initialized:
            try:
                import ee
                import geemap

                crop_year = crop_years[-1]
                rp_band = f"RP{rp}_depth"

                glofas = ee.ImageCollection("JRC/CEMS_GLOFAS/FloodHazard/v2_1")
                flood_img = glofas.select(rp_band).first()
                flood_mask = flood_img.gt(flood_threshold).rename("flooded")

                dw = ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
                chunk_size = 5
                start_idx = 0

                while start_idx < len(unit_gdf):
                    end_idx = min(start_idx + chunk_size, len(unit_gdf))
                    gdf_chunk = unit_gdf.iloc[start_idx:end_idx]

                    fc = geemap.geopandas_to_ee(gdf_chunk)

                    def add_flood_crop_stats(feature):
                        geom = feature.geometry()

                        crop_coll = (
                            dw.filterDate(f"{crop_year}-01-01", f"{crop_year}-12-31")
                            .filterBounds(geom)
                            .select("label")
                        )
                        crop_composite = crop_coll.reduce(ee.Reducer.mode())
                        crop_mask = crop_composite.eq(4).rename("crop")

                        pixel_area = ee.Image.pixelArea()
                        admin_area_km2 = ee.Number(geom.area()).divide(1e6)

                        flooded_crop = crop_mask.updateMask(flood_mask)

                        flooded_crop_area_m2 = (
                            flooded_crop.multiply(pixel_area)
                            .reduceRegion(
                                reducer=ee.Reducer.sum(),
                                geometry=geom,
                                scale=90,
                                bestEffort=True,
                            )
                            .get("crop")
                        )
                        flooded_crop_km2 = ee.Number(flooded_crop_area_m2).divide(1e6)

                        total_crop_area_m2 = (
                            crop_mask.multiply(pixel_area)
                            .reduceRegion(
                                reducer=ee.Reducer.sum(),
                                geometry=geom,
                                scale=10,
                                bestEffort=True,
                            )
                            .get("crop")
                        )
                        total_crop_km2 = ee.Number(total_crop_area_m2).divide(1e6)

                        areapct = flooded_crop_km2.divide(admin_area_km2).multiply(100)
                        croppct = ee.Algorithms.If(
                            total_crop_km2.gt(0),
                            flooded_crop_km2.divide(total_crop_km2).multiply(100),
                            0,
                        )

                        return feature.set(
                            {
                                "crop_km2": flooded_crop_km2,
                                "crop_areapct": areapct,
                                "crop_croppct": croppct,
                            }
                        )

                    fc_stats = fc.map(add_flood_crop_stats)
                    fc_out = fc_stats.select(
                        propertySelectors=[
                            f"{admin_level}_PCODE",
                            "crop_km2",
                            "crop_areapct",
                            "crop_croppct",
                        ],
                        retainGeometry=False,
                    )

                    temp_csv = temp_dir / (
                        f"flood_crop_RP{rp}{chunk_tag}_chunk{start_idx}.csv"
                    )
                    geemap.ee_to_csv(fc_out, filename=str(temp_csv))

                    df_chunk = pd.read_csv(temp_csv)
                    for c in ["crop_km2", "crop_areapct", "crop_croppct"]:
                        df_chunk[c] = pd.to_numeric(
                            df_chunk[c], errors="coerce"
                        ).fillna(0)

                    for _, row_cf in df_chunk.iterrows():
                        pcode = row_cf[f"{admin_level}_PCODE"]
                        match = rp_df[rp_df[f"{admin_level}_PCODE"] == pcode].index
                        if not match.empty:
                            i = match[0]
                            rp_df.loc[i, f"RP{rp}_crops_{thresh_suffix}_km2"] = round(
                                row_cf["crop_km2"], 2
                            )
                            rp_df.loc[i, f"RP{rp}_crops_{thresh_suffix}_areapct"] = (
                                round(row_cf["crop_areapct"], 2)
                            )
                            rp_df.loc[i, f"RP{rp}_crops_{thresh_suffix}_croppct"] = (
                                round(row_cf["crop_croppct"], 2)
                            )

                    os.remove(temp_csv)
                    start_idx = end_idx
                    context.info(
                        f"Crops chunk {start_idx // chunk_size}/{-(-len(unit_gdf) // chunk_size)} done for RP{rp}"
                    )

                context.info(
                    f"Processed flooded crops >{flood_threshold} m ({thresh_suffix})"
                )

            except Exception as e:
                context.warning(f"Flooded crops computation failed: {e}")
                rp_df[f"RP{rp}_crops_{thresh_suffix}_km2"] = 0.0
                rp_df[f"RP{rp}_crops_{thresh_suffix}_areapct"] = 0.0
                rp_df[f"RP{rp}_crops_{thresh_suffix}_croppct"] = 0.0

    # ---- Flooded population ----
    # All WorldPop indicator rasters share the same grid, so a single shared
    # 1 km flood mask is used for every indicator. When a shared mask file
    # exists it is read directly; otherwise the whole-country flood raster is
    # reprojected onto the country-wide 1 km grid here. The full-resolution
    # mosaic is never held in memory per chunk (it is multi-GB for large
    # countries), so it is only opened inside the reprojection fallbacks.
    ref_pop = rioxarray.open_rasterio(tif_map[indicators[0]], masked=True).squeeze()
    if flood_mask_path is not None:
        depth_aligned = rioxarray.open_rasterio(flood_mask_path, masked=True).squeeze()
        flood_mask = (depth_aligned > flood_threshold).astype("float32")
        del depth_aligned
    else:
        flood_aligned = (
            rioxarray.open_rasterio(clipped_path, masked=True)
            .squeeze()
            .rio.reproject_match(ref_pop, resampling=Resampling.bilinear)
        )
        flood_mask = (flood_aligned > flood_threshold).astype("float32")
        del flood_aligned

    for label in indicators:
        pop_raster_path = tif_map[label]
        pop_raster = rioxarray.open_rasterio(pop_raster_path, masked=True).squeeze()

        if (
            pop_raster.shape != ref_pop.shape
            or pop_raster.rio.transform() != ref_pop.rio.transform()
            or pop_raster.rio.crs != ref_pop.rio.crs
        ):
            flood_aligned_label = (
                rioxarray.open_rasterio(clipped_path, masked=True)
                .squeeze()
                .rio.reproject_match(pop_raster, resampling=Resampling.bilinear)
            )
            flood_mask_label = (flood_aligned_label > flood_threshold).astype("float32")
            flooded_pop = pop_raster * flood_mask_label
        else:
            flooded_pop = pop_raster * flood_mask

        stats = zonal_stats(
            unit_gdf,
            flooded_pop.values.astype(np.float32),
            affine=pop_raster.rio.transform(),
            stats="sum",
            nodata=0,
        )
        rp_df[f"RP{rp}_{label}_{thresh_suffix}"] = [
            s["sum"] if s["sum"] is not None else 0 for s in stats
        ]

        context.info(
            f"Processed flooded population for {label} >{flood_threshold} m ({thresh_suffix}){chunk_label}"
        )

    # Calculate mathematical dependency ratio for the flooded population
    dep_col_num = rp_df[f"RP{rp}_dep_dependents_{thresh_suffix}"]
    dep_col_den = rp_df[f"RP{rp}_dep_working_{thresh_suffix}"].replace(0, pd.NA)
    rp_df[f"RP{rp}_dependency_ratio_{thresh_suffix}"] = (
        ((dep_col_num / dep_col_den) * 100).fillna(0).round(2)
    )

    # Drop the intermediate components
    rp_df.drop(
        columns=[
            f"RP{rp}_dep_dependents_{thresh_suffix}",
            f"RP{rp}_dep_working_{thresh_suffix}",
        ],
        inplace=True,
    )

    # ---- Flooded facilities ----
    with rasterio.open(clipped_path) as src:
        for category, filepath in geojsons_map.items():
            if not Path(filepath).exists():
                rp_df[f"RP{rp}_{category}_{thresh_suffix}_pct"] = 0
                rp_df[f"RP{rp}_{category}_{thresh_suffix}_count"] = 0
                continue

            facilities = gpd.read_file(filepath)
            if facilities.empty:
                rp_df[f"RP{rp}_{category}_{thresh_suffix}_pct"] = 0
                rp_df[f"RP{rp}_{category}_{thresh_suffix}_count"] = 0
                continue

            if not all(facilities.geometry.type == "Point"):
                if facilities.crs != src.crs:
                    facilities = facilities.to_crs(src.crs)
                facilities["geometry"] = facilities.geometry.centroid
            if facilities.crs != src.crs:
                facilities = facilities.to_crs(src.crs)

            coords = [
                (x, y) for x, y in zip(facilities.geometry.x, facilities.geometry.y)
            ]
            values = [v[0] for v in src.sample(coords)]
            facilities["flooded"] = [1 if v > flood_threshold else 0 for v in values]

            joined = gpd.sjoin(
                facilities,
                unit_gdf[[f"{admin_level}_PCODE", "geometry"]],
                how="inner",
                predicate="within",
            )
            grouped = (
                joined.groupby(f"{admin_level}_PCODE")["flooded"]
                .agg(["mean", "sum"])
                .reset_index()
            )

            grouped[f"RP{rp}_{category}_{thresh_suffix}_pct"] = (
                grouped["mean"] * 100
            ).round(1)
            grouped[f"RP{rp}_{category}_{thresh_suffix}_count"] = grouped["sum"].astype(
                int
            )
            grouped = grouped.drop(columns=["mean", "sum"])

            rp_df = rp_df.merge(grouped, on=f"{admin_level}_PCODE", how="left")
            rp_df[f"RP{rp}_{category}_{thresh_suffix}_pct"] = rp_df[
                f"RP{rp}_{category}_{thresh_suffix}_pct"
            ].fillna(0)
            rp_df[f"RP{rp}_{category}_{thresh_suffix}_count"] = (
                rp_df[f"RP{rp}_{category}_{thresh_suffix}_count"].fillna(0).astype(int)
            )

            context.info(
                f"Processed flooded facilities for {category} >{flood_threshold} m ({thresh_suffix}){chunk_label}"
            )

    return rp_df


def process_flood_impact(
    context,
    country_code,
    rps,
    gdf,
    admin_level,
    output_dir,
    flood_threshold=0.3,
    api_choice="ohsome-api",
    crop_years=None,
    chunking=False,
    chunk_max_cells=None,
    res_deg=None,
):
    """
    Process flooded population, crops, and facilities for all RPs of a given
    country/admin_level. Generates a single CSV with columns for each RP,
    indicator, threshold, flooded cropland area, and flooded facilities.
    Flooded cropland area is computed server-side in GEE: the flood extent
    per admin unit is uploaded as EE geometries and intersected with
    Dynamic World crop classification via reduceRegion.

    The country-wide flood mosaic is written to disk with a streaming merge
    (memory bounded by a single clipped tile) and reprojected once onto the
    shared WorldPop 1 km grid. If `chunking` is enabled and the country's
    raster footprint (at `res_deg`) exceeds `chunk_max_cells`, the admin
    units are split into row-contiguous chunks and each chunk is processed
    against that single shared 1 km flood mask, so memory stays bounded.
    Per-unit stats are additive, so the concatenated chunk results produce
    the same CSV as a whole-country run.
    """
    country_code = country_code.upper()
    gdf = to_4326(gdf)
    output_dir = Path(output_dir)
    out_csv = output_dir / f"{country_code}_{admin_level}_flood_exposure.csv"
    temp_dir = Path("data") / country_code / "Temporary"

    THRESH_SUFFIX = f"{int(flood_threshold * 100)}cm"

    if crop_years is None:
        crop_years = []

    # Load existing CSV if present, to append missing RPs
    if out_csv.exists():
        context.info(f"CSV exists: {out_csv}, will append missing RPs")
        final_df = pd.read_csv(out_csv)
    else:
        final_df = pd.DataFrame({f"{admin_level}_PCODE": gdf[f"{admin_level}_PCODE"]})

    # Ensure WorldPop files exist
    context.info(f"Ensuring demographic rasters exist in {temp_dir}...")
    indicator_tifs = fetch_worldpop(country_code)
    indicators = [
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
    tif_map = dict(zip(INDICATORS.keys(), indicator_tifs))

    # We will use this list to check expected columns so we don't look for deleted columns
    final_indicators = [
        "female_pop",
        "children_u5",
        "female_u5",
        "elderly",
        "pop_u15",
        "female_u15",
        "wra_pop",
        "dependency_ratio",
    ]

    # Ensure facilities exist
    context.info(f"Ensuring facility raw geometries exist in {temp_dir}...")
    base_path = Path("data") / country_code
    boundary_path = base_path / f"{country_code}_{admin_level}.geojson"
    api_choice = api_choice.lower()

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
            geojsons_map[category] = (
                base_path / f"Temporary/{country_code}_{category}_raw.geojson"
            )

    ee_initialized = False
    if crop_years:
        try:
            import ee

            ee.Initialize(project="aa-automatization")
            ee_initialized = True
        except Exception:
            ee_initialized = False

    for rp in rps:
        context.info(f"Processing RP{rp}...")

        # Skip RP if all expected columns already exist
        expected_cols = (
            [
                f"RP{rp}_{label}_{suffix}"
                for label in final_indicators
                for suffix in [THRESH_SUFFIX]
            ]
            + [
                f"RP{rp}_{cat}_{suffix}_pct"
                for cat in facility_categories
                for suffix in [THRESH_SUFFIX]
            ]
            + [
                f"RP{rp}_{cat}_{suffix}_count"
                for cat in facility_categories
                for suffix in [THRESH_SUFFIX]
            ]
            + [f"RP{rp}_crops_{suffix}_km2" for suffix in [THRESH_SUFFIX]]
            + [f"RP{rp}_crops_{suffix}_areapct" for suffix in [THRESH_SUFFIX]]
            + [f"RP{rp}_crops_{suffix}_croppct" for suffix in [THRESH_SUFFIX]]
        )
        if all(col in final_df.columns for col in expected_cols):
            context.info(f"RP{rp} already processed, skipping...")
            continue

        # Decide whether the country's flood footprint is too large to hold in
        # memory at once. If so, split the admin units into row-contiguous
        # chunks so each chunk's per-unit computation stays within
        # chunk_max_cells. The flood raster itself is always built once for
        # the whole country, but written to disk via a streaming merge, so its
        # memory stays bounded by a single clipped tile.
        footprint_cells = estimate_raster_cells(gdf, res_deg) if res_deg else 0
        chunking_active = bool(
            chunking
            and res_deg
            and chunk_max_cells
            and footprint_cells > chunk_max_cells
        )
        if chunking_active:
            units_groups = _split_into_chunks(gdf, res_deg, chunk_max_cells)
            context.info(
                f"Chunking {country_code}: ~{footprint_cells:,} raster cells exceed "
                f"CHUNK_MAX_CELLS ({chunk_max_cells:,}) → splitting into "
                f"{len(units_groups)} chunks."
            )
        else:
            units_groups = [gdf]

        # Build the country-wide clipped flood mosaic once (streaming merge).
        clipped_path = process_country_rp(context, country_code, rp, admin_level)
        if not clipped_path:
            context.warning(f"No flood raster for RP{rp}, skipping...")
            continue

        # Build the shared 1 km flood mask once. Every chunk (and the
        # whole-country run) reads this single file, so per-unit population
        # sums are identical regardless of chunking.
        mask_path = os.path.join(temp_dir, f"{country_code}_flooded_RP{rp}_1km.tif")
        if not os.path.exists(mask_path):
            _build_flood_mask_1km(
                context,
                country_code,
                rp,
                clipped_path,
                mask_path,
                tif_map[indicators[0]],
            )
        else:
            context.info(f"Flood 1 km mask already exists: {mask_path}")

        rp_chunk_dfs = []
        for ci, unit_gdf in enumerate(units_groups):
            chunk_tag = f"_chunk{ci}" if chunking_active else ""
            chunk_label = (
                f" [chunk {ci + 1}/{len(units_groups)}]" if chunking_active else ""
            )
            rp_chunk_dfs.append(
                _compute_rp_exposure(
                    context=context,
                    country_code=country_code,
                    rp=rp,
                    unit_gdf=unit_gdf,
                    admin_level=admin_level,
                    temp_dir=temp_dir,
                    clipped_path=clipped_path,
                    flood_mask_path=mask_path,
                    flood_threshold=flood_threshold,
                    thresh_suffix=THRESH_SUFFIX,
                    tif_map=tif_map,
                    indicators=indicators,
                    geojsons_map=geojsons_map,
                    crop_years=crop_years,
                    ee_initialized=ee_initialized,
                    chunk_tag=chunk_tag,
                    chunk_label=chunk_label,
                )
            )

        if not rp_chunk_dfs:
            continue

        # Per-unit stats are additive: concatenating per-chunk results on PCODE
        # yields the same values as processing the whole country at once.
        if len(rp_chunk_dfs) == 1:
            rp_df = rp_chunk_dfs[0]
        else:
            rp_df = pd.concat(rp_chunk_dfs, ignore_index=True)

        final_df = final_df.merge(rp_df, on=f"{admin_level}_PCODE", how="left")
        context.info(f"Processed RP{rp}")

    numeric_cols = final_df.select_dtypes(include=["float", "int"]).columns
    crops_cols = [c for c in final_df.columns if "_crops_" in c]
    int_cols = [c for c in numeric_cols if c not in crops_cols]
    final_df[int_cols] = final_df[int_cols].fillna(0).round(0).astype(int)
    if crops_cols:
        final_df[crops_cols] = final_df[crops_cols].fillna(0)
    output_dir.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(out_csv, index=False)
    context.info(f"Flooded population, crops & facilities CSV written to {out_csv}")

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
        help="Administrative level (ADM0, ADM1, ADM2, etc.). Default is ADM0",
    )
    parser.add_argument(
        "--rp",
        help=f"Return period (allowed: {', '.join(ALLOWED_RPS)}). If omitted, all allowed RPs are processed.",
        default=None,
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    log = logging.getLogger(__name__)

    country_code = args.country_code.upper()
    admin_level = args.admin_level.upper()

    rps_to_process = [args.rp.strip()] if args.rp else ALLOWED_RPS
    for rp in rps_to_process:
        if rp not in ALLOWED_RPS:
            raise ValueError(
                f"Invalid RP '{rp}'. Allowed values: {', '.join(ALLOWED_RPS)}"
            )

        log.info("=== Processing %s, %s, RP%s ===", country_code, admin_level, rp)

        clipped_path = process_country_rp(log, country_code, rp, admin_level)

        temporary_dir = f"data/{country_code}/Temporary"
        output_dir = f"data/{country_code}/Output"
        gdf_path = os.path.join(
            "data", country_code, f"{country_code}_{admin_level}.geojson"
        )
        if not os.path.exists(gdf_path):
            raise FileNotFoundError(f"Boundary file not found: {gdf_path}")
        gdf = gpd.read_file(gdf_path)

        process_flood_impact(
            context=log,
            country_code=country_code,
            rps=[rp],
            gdf=gdf,
            admin_level=admin_level,
            output_dir=output_dir,
        )
