import os
import geopandas as gpd
import pandas as pd
import numpy as np
import rasterio
from rasterio.features import shapes as raster_shapes
from rasterio.mask import mask as rio_mask
from rasterstats import zonal_stats
from shapely.geometry import shape
from shapely.ops import unary_union
from pathlib import Path


FINAL_INDICATORS = [
    "total_pop", "female_pop", "children_u5", "female_u5",
    "elderly", "pop_u15", "female_u15", "wra_pop", "working", "dependents"
]


def _estimate_utm_crs(gdf: gpd.GeoDataFrame) -> str:
    lon = gdf.to_crs(epsg=4326).geometry.centroid.x.iloc[0]
    utm_zone = int((lon + 180) // 6) + 1
    hemisphere = "south" if gdf.to_crs(epsg=4326).geometry.centroid.y.iloc[0] < 0 else "north"
    epsg_code = 32700 + utm_zone if hemisphere == "south" else 32600 + utm_zone
    return f"epsg:{epsg_code}"


def compute_rural_access_index(
    country_code: str,
    admin_level: str,
    boundary_dir: str,
    demographics_csv: str,
    population_raster_dir: str,
    mapillary_path: str,
    planet_path: str,
    smod_raster_path: str,
    output_dir: str,
    context,
) -> str:
    """
    Compute Rural Accessibility Index (RAI).
    Mirrors rural_access_index.R methodology exactly.
    """
    country_code = country_code.upper()

    # 1. Load boundaries
    adm0_path = os.path.join(boundary_dir, f"{country_code}_ADM0.geojson")
    adm_path = os.path.join(boundary_dir, f"{country_code}_{admin_level}.geojson")

    if not os.path.exists(adm0_path):
        raise FileNotFoundError(f"ADM0 boundary not found: {adm0_path}")
    if not os.path.exists(adm_path):
        raise FileNotFoundError(f"{admin_level} boundary not found: {adm_path}")

    country_boundary = gpd.read_file(adm0_path)
    gdf_adm = gpd.read_file(adm_path)

    id_col = f"{admin_level}_PCODE"
    if id_col not in gdf_adm.columns:
        raise ValueError(f"Expected column '{id_col}' not found in {adm_path}")

    context.info(f"[{country_code}] Loaded ADM0 and {admin_level} boundaries")

    # 2. Load road data, filter paved, merge
    context.info(f"[{country_code}] Loading road data")
    mapillary = gpd.read_file(mapillary_path)
    planet = gpd.read_file(planet_path)

    mly_paved = mapillary[mapillary["pred_label"] == 0].copy()
    plt_paved = planet[planet["DL_road_class_2024"] == "paved"].copy()

    mly_geom = mly_paved[["geometry"]]
    plt_geom = plt_paved[["geometry"]]

    merged_roads = gpd.GeoDataFrame(
        pd.concat([mly_geom, plt_geom], ignore_index=True),
        crs=mly_paved.crs
    ).to_crs(epsg=4326)

    # 3. Project and buffer roads by 2km, then dissolve
    utm_crs = _estimate_utm_crs(merged_roads)
    buffered = merged_roads.to_crs(utm_crs).buffer(2000).union_all()
    buffered_roads = gpd.GeoDataFrame(
        geometry=[buffered], crs=utm_crs
    ).to_crs(epsg=4326)

    context.info(f"[{country_code}] Buffered roads by 2km")

    # 4. Load SMOD, crop to country, extract rural classes (11,12,13)
    context.info(f"[{country_code}] Processing SMOD raster")
    country_proj = country_boundary.to_crs(rasterio.open(smod_raster_path).crs)

    with rasterio.open(smod_raster_path) as src:
        out_image, out_transform = rio_mask(src, country_proj.geometry, crop=True)
        smod_array = out_image[0]

    # Classify: 11,12,13 -> 1; everything else -> 0
    rural_mask = np.isin(smod_array, [11, 12, 13])
    smod_binary = np.where(rural_mask, 1, 0).astype(np.uint8)

    # Convert rural pixels to vector polygons, dissolve into single polygon
    results = (
        {"properties": {"val": v}, "coordinates": c}
        for c, v in raster_shapes(smod_binary, transform=out_transform)
        if v == 1
    )
    rural_polys = [shape(r["coordinates"]) for r in results]

    if not rural_polys:
        context.warning(f"[{country_code}] No rural SMOD pixels found — all RAI values will be 0")
        smod_single = gpd.GeoDataFrame(geometry=[], crs=src.crs)
    else:
        dissolved = unary_union(rural_polys)
        smod_single = gpd.GeoDataFrame(geometry=[dissolved], crs=src.crs)

    smod_4326 = smod_single.to_crs(epsg=4326)

    # 5. Intersect buffered roads with rural SMOD polygon
    if smod_4326.empty or buffered_roads.empty:
        context.warning(f"[{country_code}] No overlap between roads and rural areas")
        accessible_rural = gpd.GeoDataFrame(geometry=[], crs="epsg:4326")
    else:
        smod_clean = gpd.make_valid(smod_4326)
        buffered_clean = gpd.make_valid(buffered_roads)
        accessible = gpd.overlay(buffered_clean, smod_clean, how="intersection")
        accessible_rural = gpd.make_valid(accessible)

    context.info(f"[{country_code}] Computed accessible rural areas")

    # 6. Intersect accessible areas with ADM2 boundaries
    if accessible_rural.empty:
        context.warning(f"[{country_code}] No accessible rural areas — returning zeros")
        areas_by_adm2 = gdf_adm[[id_col]].copy()
        for ind in FINAL_INDICATORS:
            areas_by_adm2[f"rural_access_{ind}"] = 0
        areas_by_adm2["rural_access_dependency_ratio"] = np.nan
    else:
        gdf_adm_4326 = gdf_adm.to_crs(epsg=4326)
        gdf_adm_4326 = gpd.make_valid(gdf_adm_4326)
        accessible_rural = gpd.make_valid(accessible_rural)

        areas_by_adm2 = gpd.overlay(accessible_rural, gdf_adm_4326, how="intersection")
        areas_by_adm2 = areas_by_adm2[[id_col, "geometry"]].copy()

        # 7. Extract population from rasters for each indicator
        pop_extraction = pd.DataFrame({id_col: areas_by_adm2[id_col].unique()})

        for indicator in FINAL_INDICATORS:
            raster_path = _find_indicator_raster(population_raster_dir, indicator, country_code)
            if raster_path is None or not os.path.exists(raster_path):
                context.warning(f"[{country_code}] Raster not found for indicator '{indicator}', skipping")
                pop_extraction[f"rural_access_{indicator}"] = 0
                continue

            stats = zonal_stats(
                areas_by_adm2.to_crs(rasterio.open(raster_path).crs),
                raster_path,
                stats="sum",
                nodata=0,
            )
            values = [s["sum"] if s and s["sum"] is not None else 0 for s in stats]

            adm2_sums = pd.DataFrame({id_col: areas_by_adm2[id_col], "_val": values})
            adm2_sums = adm2_sums.groupby(id_col)["_val"].sum().reset_index()
            adm2_sums.columns = [id_col, f"rural_access_{indicator}"]

            pop_extraction = pop_extraction.merge(adm2_sums, on=id_col, how="left")
            pop_extraction[f"rural_access_{indicator}"] = (
                pop_extraction[f"rural_access_{indicator}"].fillna(0).round(0).astype(int)
            )
            context.info(f"[{country_code}] Extracted rural_access_{indicator}")

        # Build full ADM2 result with all rows
        result = gdf_adm[[id_col, "geometry"]].copy()
        result = result.merge(pop_extraction, on=id_col, how="left")

        # Fill NaN with 0 for all rural_access columns
        access_cols = [c for c in result.columns if c.startswith("rural_access_")]
        for c in access_cols:
            result[c] = result[c].fillna(0).astype(int)

        # 8. Load demographics CSV for total population
        if os.path.exists(demographics_csv):
            demo = pd.read_csv(demographics_csv)
            available = [c for c in FINAL_INDICATORS if c in demo.columns]
            result = result.merge(
                demo[[id_col] + available], on=id_col, how="left"
            )

            # 9. Calculate RAI ratios
            for ind in available:
                access_col = f"rural_access_{ind}"
                total_col = ind
                ratio_col = f"RAI_{ind}"
                # RAI = (accessible / total) * 100
                result[ratio_col] = np.where(
                    result[total_col] > 0,
                    ((result[access_col] / result[total_col]) * 100).round(1),
                    0,
                )

        # 10. Calculate dependency ratio (from raster counts directly)
        result["rural_access_dependency_ratio"] = np.where(
            result["rural_access_working"] > 0,
            (result["rural_access_dependents"] / result["rural_access_working"] * 100).round(1),
            np.nan,
        )

        areas_by_adm2 = result

    # 11. Select final columns and save CSV
    output_cols = [id_col]
    output_cols += [c for c in areas_by_adm2.columns if c.startswith("rural_access_")]
    output_cols += [c for c in areas_by_adm2.columns if c.startswith("RAI_")]

    rai_csv = areas_by_adm2[output_cols].copy()

    # Drop geometry
    if "geometry" in rai_csv.columns:
        rai_csv = rai_csv.drop(columns=["geometry"])

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{country_code}_{admin_level}_rural_access_index.csv")
    rai_csv.to_csv(output_path, index=False)
    context.info(f"[{country_code}] RAI CSV written to {output_path}")

    return output_path


def _find_indicator_raster(raster_dir: str, indicator: str, country_code: str) -> str:
    """
    Find the WorldPop raster for a given indicator.
    Handles indicator name mapping: dependents -> dep_dependents, working -> dep_working.
    """
    indicator_map = {
        "dependents": "dep_dependents",
        "working": "dep_working",
    }
    mapped = indicator_map.get(indicator, indicator)
    pattern = f"{country_code}_pop_{mapped}_*_constrained.tif"
    import glob
    matches = glob.glob(os.path.join(raster_dir, pattern))
    if matches:
        return matches[0]

    # Fallback: search for any tif containing the indicator name
    for f in os.listdir(raster_dir):
        if f.endswith(".tif") and mapped in f and country_code in f:
            return os.path.join(raster_dir, f)

    return None
