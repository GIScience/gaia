import os
import shutil
from pathlib import Path
import geopandas as gpd
import pandas as pd
import numpy as np
import rioxarray
import rasterio
import requests
from rasterstats import zonal_stats
from scripts.fetch_ruralness_ghsl import download_and_unzip_smod, reclassify_raster, RECLASS_MAP
from scripts.fetch_worldpop import fetch_worldpop, INDICATORS as WP_INDICATORS

RAI_INDICATORS = [
    "total_pop", "female_pop", "children_u5", "female_u5",
    "elderly", "pop_u15", "female_u15", "wra_pop",
    "dep_dependents", "dep_working",
]

OUTPUT_NAME_MAP = {
    "total_pop": "total_pop",
    "female_pop": "female_pop",
    "children_u5": "children_u5",
    "female_u5": "female_u5",
    "elderly": "elderly",
    "pop_u15": "pop_u15",
    "female_u15": "female_u15",
    "wra_pop": "wra_pop",
    "dep_dependents": "dependents",
    "dep_working": "working",
}

ISO3_TO_ISO2 = {
    "AFG": "AF", "AGO": "AO", "ALB": "AL", "ARE": "AE", "ARG": "AR",
    "ARM": "AM", "ATG": "AG", "AZE": "AZ", "BDI": "BI", "BEN": "BJ",
    "BFA": "BF", "BGD": "BD", "BGR": "BG", "BHR": "BH", "BHS": "BS",
    "BLR": "BY", "BLZ": "BZ", "BOL": "BO", "BRA": "BR", "BRB": "BB",
    "BTN": "BT", "BWA": "BW", "CAF": "CF", "CHL": "CL", "CHN": "CN",
    "CIV": "CI", "CMR": "CM", "COD": "CD", "COG": "CG", "COL": "CO",
    "COM": "KM", "CPV": "CV", "CRI": "CR", "CUB": "CU", "DJI": "DJ",
    "DMA": "DM", "DOM": "DO", "DZA": "DZ", "ECU": "EC", "EGY": "EG",
    "ERI": "ER", "ESH": "EH", "ETH": "ET", "FJI": "FJ", "FSM": "FM",
    "GAB": "GA", "GEO": "GE", "GHA": "GH", "GIN": "GN", "GMB": "GM",
    "GNB": "GW", "GNQ": "GQ", "GRC": "GR", "GRD": "GD", "GTM": "GT",
    "GUY": "GY", "HND": "HN", "HTI": "HT", "HUN": "HU", "IDN": "ID",
    "IRN": "IR", "IRQ": "IQ", "JAM": "JM", "KAZ": "KZ", "KEN": "KE",
    "KGZ": "KG", "KHM": "KH", "KIR": "KI", "KNA": "KN", "KWT": "KW",
    "LAO": "LA", "LBN": "LB", "LBR": "LR", "LBY": "LY", "LCA": "LC",
    "LKA": "LK", "LSO": "LS", "MAR": "MA", "MDA": "MD", "MDG": "MG",
    "MDV": "MV", "MEX": "MX", "MHL": "MH", "MKD": "MK", "MLI": "ML",
    "MMR": "MM", "MNG": "MN", "MOZ": "MZ", "MRT": "MR", "MUS": "MU",
    "MWI": "MW", "MYS": "MY", "NAM": "NA", "NER": "NE", "NGA": "NG",
    "NIC": "NI", "NPL": "NP", "OMN": "OM", "PAK": "PK", "PAN": "PA",
    "PER": "PE", "PHL": "PH", "PNG": "PG", "POL": "PL", "PRK": "KP",
    "PRY": "PY", "QAT": "QA", "ROU": "RO", "RUS": "RU", "RWA": "RW",
    "SAU": "SA", "SDN": "SD", "SEN": "SN", "SLB": "SB", "SLE": "SL",
    "SLV": "SV", "SOM": "SO", "SSD": "SS", "STP": "ST", "SUR": "SR",
    "SVK": "SK", "SWZ": "SZ", "SYC": "SC", "SYR": "SY", "TCD": "TD",
    "TGO": "TG", "THA": "TH", "TJK": "TJ", "TLS": "TL", "TON": "TO",
    "TTO": "TT", "TUN": "TN", "TUR": "TR", "TZA": "TZ", "UGA": "UG",
    "UKR": "UA", "URY": "UY", "UZB": "UZ", "VCT": "VC", "VEN": "VE",
    "VNM": "VN", "VUT": "VU", "YEM": "YE", "ZAF": "ZA", "ZMB": "ZM",
    "ZWE": "ZW",
}


def country_iso2(country_code):
    return ISO3_TO_ISO2.get(country_code.upper(), country_code[:2])


def download_road_data(country_code, download_dir, context):
    country_code = country_code.upper()
    iso3_lower = country_code.lower()
    iso2 = country_iso2(country_code).lower()
    download_dir = Path(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)

    mapillary_url = (
        f"https://downloads.ohsome.org/hdx/mapillary_road_surface/"
        f"heigit_{iso3_lower}_roadsurface_lines.gpkg"
    )
    planet_url = (
        f"https://hot.storage.heigit.org/heigit-hdx-public/planet_road_data/"
        f"heigit_{iso2.upper()}_planet_roadsurface_lines.gpkg"
    )

    mapillary_path = download_dir / f"heigit_{iso3_lower}_roadsurface_lines.gpkg"
    planet_path = download_dir / f"heigit_{iso2.upper()}_planet_roadsurface_lines.gpkg"

    paths = {"mapillary": None, "planet": None}

    if mapillary_path.exists():
        context.info(f"Mapillary data already cached: {mapillary_path}")
        paths["mapillary"] = str(mapillary_path)
    else:
        context.info(f"Downloading Mapillary roads from {mapillary_url}")
        resp = requests.get(mapillary_url, stream=True, timeout=300)
        if resp.status_code == 200:
            with open(mapillary_path, "wb") as f:
                for chunk in resp.iter_content(1024 * 1024):
                    f.write(chunk)
            paths["mapillary"] = str(mapillary_path)
            context.info(f"Saved Mapillary roads to {mapillary_path}")
        else:
            context.warning(f"Mapillary data not available (HTTP {resp.status_code}): {mapillary_url}")

    if planet_path.exists():
        context.info(f"Planet data already cached: {planet_path}")
        paths["planet"] = str(planet_path)
    else:
        context.info(f"Downloading Planet roads from {planet_url}")
        resp = requests.get(planet_url, stream=True, timeout=300)
        if resp.status_code == 200:
            with open(planet_path, "wb") as f:
                for chunk in resp.iter_content(1024 * 1024):
                    f.write(chunk)
            paths["planet"] = str(planet_path)
            context.info(f"Saved Planet roads to {planet_path}")
        else:
            context.warning(f"Planet data not available (HTTP {resp.status_code}): {planet_url}")

    return paths


def _estimate_utm(gdf):
    centroid = gdf.dissolve().centroid.iloc[0]
    lon, lat = centroid.x, centroid.y
    utm_zone = int((lon + 180) / 6) + 1
    return f"EPSG:{32600 + utm_zone if lat >= 0 else 32700 + utm_zone}"


# Desired output column order (matching the original R script output)
RAI_OUTPUT_COLUMNS = [
    "rural_access_children_u5",
    "rural_access_dependents",
    "rural_access_working",
    "rural_access_elderly",
    "rural_access_female_pop",
    "rural_access_female_u15",
    "rural_access_female_u5",
    "rural_access_pop_u15",
    "rural_access_total_pop",
    "rural_access_wra_pop",
    "rural_access_dependency_ratio",
    "RAI_total_pop",
    "RAI_female_pop",
    "RAI_children_u5",
    "RAI_female_u5",
    "RAI_elderly",
    "RAI_pop_u15",
    "RAI_female_u15",
    "RAI_wra_pop",
]


def compute_rai(country_code, admin_level, gdf_admin, output_dir, work_dir,
                mapillary_path, planet_path, demographics_csv, rural_csv, context):
    country_code = country_code.upper()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = work_dir / "rai_temp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    id_col = f"{admin_level}_PCODE"
    if id_col not in gdf_admin.columns:
        raise ValueError(f"Column {id_col} not found in admin boundaries")

    out_csv = output_dir / f"{country_code}_{admin_level}_rai.csv"
    if out_csv.exists():
        context.info(f"RAI CSV already exists, skipping: {out_csv}")
        return str(out_csv)

    # ----------------------------------------------------------------
    # 1. Load road surface data (Mapillary / Planet)
    # ----------------------------------------------------------------
    all_roads = []

    if mapillary_path and os.path.exists(mapillary_path):
        mly = gpd.read_file(mapillary_path)
        if "pred_label" in mly.columns:
            mly = mly[mly["pred_label"] == 0]
        all_roads.append(mly.to_crs(4326))
        context.info(f"Mapillary paved roads: {len(mly)} features")
    else:
        context.warning(f"Mapillary path not available: {mapillary_path}")

    if planet_path and os.path.exists(planet_path):
        plt = gpd.read_file(planet_path)
        if "DL_road_class_2024" in plt.columns:
            plt = plt[plt["DL_road_class_2024"] == "paved"]
        all_roads.append(plt.to_crs(4326))
        context.info(f"Planet paved roads: {len(plt)} features")
    else:
        context.warning(f"Planet path not available: {planet_path}")

    if not all_roads:
        context.warning(f"No road data found for {country_code} — writing empty output")
        pd.DataFrame({id_col: gdf_admin[id_col]}).to_csv(out_csv, index=False)
        return str(out_csv)

    merged_roads = pd.concat(all_roads, ignore_index=True)

    # ----------------------------------------------------------------
    # 2. Buffer paved roads by 2 km
    # ----------------------------------------------------------------
    context.info("Buffering paved roads by 2 km")
    utm_crs = _estimate_utm(gdf_admin)
    buffered = (
        merged_roads[["geometry"]]
        .to_crs(utm_crs)
        .buffer(2000)
        .union_all()
    )
    buffered_gdf = gpd.GeoDataFrame(geometry=[buffered], crs=utm_crs).to_crs(4326)

    context.info("Intersecting buffered roads with admin boundaries")
    roads_by_adm = gpd.overlay(buffered_gdf, gdf_admin[[id_col, "geometry"]], how="intersection")
    if roads_by_adm.empty:
        context.warning("No road-admin intersection found — writing empty output")
        pd.DataFrame({id_col: gdf_admin[id_col]}).to_csv(out_csv, index=False)
        return str(out_csv)

    # ----------------------------------------------------------------
    # 3. Fetch / cache the global GHS-SMOD reclassified raster
    # ----------------------------------------------------------------
    context.info("Processing GHS-SMOD rural classification")
    smod_work = Path("downloads")
    smod_work.mkdir(parents=True, exist_ok=True)
    reclass_tif = smod_work / "smod_reclass.tif"

    if not reclass_tif.exists():
        _, _, smod_tif = download_and_unzip_smod(smod_work, context)
        reclassify_raster(smod_tif, reclass_tif, RECLASS_MAP, context)
    else:
        context.info("Using existing smod_reclass.tif")

    # ----------------------------------------------------------------
    # 4. Clip SMOD to country boundary before any further processing
    # ----------------------------------------------------------------
    context.info("Clipping SMOD to country boundary")
    adm0_path = Path(output_dir).parent.parent / f"{country_code}_ADM0.geojson"
    if adm0_path.exists():
        gdf_adm0 = gpd.read_file(adm0_path)
    else:
        context.info("ADM0 boundary not found — dissolving admin boundaries for SMOD clip")
        gdf_adm0 = gdf_admin.dissolve()

    smod = rioxarray.open_rasterio(reclass_tif, masked=True).squeeze()
    gdf_adm0_smod = gdf_adm0.to_crs(smod.rio.crs)
    smod_clipped = smod.rio.clip(gdf_adm0_smod.geometry, drop=True)
    rural_mask = (smod_clipped == 1).astype("float32")
    context.info(f"Clipped SMOD shape: {smod_clipped.shape}")

    # ----------------------------------------------------------------
    # 5. Polygonize rural SMOD (vectorized — matches R script approach)
    # ----------------------------------------------------------------
    context.info("Vectorizing rural SMOD to polygons")
    from rasterio.features import shapes as rasterio_shapes
    from shapely.geometry import shape

    rural_values = (smod_clipped.values == 1).astype("uint8")
    transform = smod_clipped.rio.transform()

    poly_gen = rasterio_shapes(
        rural_values,
        mask=rural_values == 1,
        transform=transform,
    )
    rural_geoms = [shape(g) for g, _ in poly_gen if g]
    context.info(f"  → {len(rural_geoms)} rural polygons")

    if not rural_geoms:
        context.warning("No rural SMOD polygons found — writing empty output")
        pd.DataFrame({id_col: gdf_admin[id_col]}).to_csv(out_csv, index=False)
        return str(out_csv)

    rural_gdf = gpd.GeoDataFrame(geometry=rural_geoms, crs=smod_clipped.rio.crs)
    rural_gdf["geometry"] = rural_gdf["geometry"].make_valid()
    rural_single = rural_gdf.dissolve().to_crs(4326)
    context.info("  → dissolved to single rural polygon")

    # ----------------------------------------------------------------
    # 6. Intersect: rural areas ∩ buffered roads → accessible rural
    # ----------------------------------------------------------------
    context.info("Intersecting rural areas with buffered roads")
    accessible_rural = gpd.overlay(buffered_gdf, rural_single, how="intersection")
    if accessible_rural.empty:
        context.warning("No intersection between rural areas and buffered roads — writing empty output")
        pd.DataFrame({id_col: gdf_admin[id_col]}).to_csv(out_csv, index=False)
        return str(out_csv)

    accessible_rural = accessible_rural.dissolve()
    accessible_rural["geometry"] = accessible_rural["geometry"].make_valid()
    context.info("  → dissolved accessible rural areas")

    # ----------------------------------------------------------------
    # 7. Intersect: accessible rural ∩ ADM2 → per admin unit
    # ----------------------------------------------------------------
    context.info("Intersecting accessible rural areas with admin boundaries")
    admin_accessible = gpd.overlay(
        gdf_admin[[id_col, "geometry"]],
        accessible_rural,
        how="intersection",
    )
    if admin_accessible.empty:
        context.warning("No accessible rural areas intersect admin boundaries — writing empty output")
        pd.DataFrame({id_col: gdf_admin[id_col]}).to_csv(out_csv, index=False)
        return str(out_csv)

    admin_accessible["geometry"] = admin_accessible["geometry"].make_valid()

    # ----------------------------------------------------------------
    # 8. Fetch WorldPop rasters & extract population counts
    # ----------------------------------------------------------------
    context.info("Fetching WorldPop rasters")
    indicator_tifs = fetch_worldpop(country_code)
    tif_map = dict(zip(WP_INDICATORS.keys(), indicator_tifs))

    context.info("Extracting population within accessible rural areas")
    results = pd.DataFrame({id_col: gdf_admin[id_col]})

    for indicator in RAI_INDICATORS:
        pop_tif = tif_map[indicator]
        context.info(f"  {indicator}")
        stats = zonal_stats(admin_accessible, pop_tif, stats="sum", nodata=0)
        output_name = OUTPUT_NAME_MAP[indicator]

        pcode_to_sum = pd.Series(
            [round(s["sum"], 0) if s["sum"] is not None else 0 for s in stats],
            index=admin_accessible[id_col].values,
        )
        results[f"rural_access_{output_name}"] = (
            results[id_col].map(pcode_to_sum).fillna(0).astype(int)
        )

    # ----------------------------------------------------------------
    # 7. Load rural population (from rural_asset) for ratio denominator
    # ----------------------------------------------------------------
    df_rural = pd.read_csv(rural_csv)
    # Rural CSV columns: total_pop_rural, female_pop_rural, etc.
    # (dep_dependents_rural / dep_working_rural were dropped — skip those)
    rural_denom = {
        ind: f"{ind}_rural"
        for ind in RAI_INDICATORS
        if ind not in ("dep_dependents", "dep_working")
    }
    rural_cols = [id_col] + list(rural_denom.values())
    available_rural_cols = [c for c in rural_cols if c in df_rural.columns]
    if available_rural_cols:
        results = results.merge(df_rural[available_rural_cols], on=id_col, how="left")

    # ----------------------------------------------------------------
    # 8. Compute RAI ratios (rural_access / rural_pop × 100)
    # ----------------------------------------------------------------
    for ind_key, output_name in OUTPUT_NAME_MAP.items():
        access_col = f"rural_access_{output_name}"
        rural_col = f"{ind_key}_rural"
        ratio_col = f"RAI_{output_name}"

        if rural_col in results.columns:
            results[ratio_col] = np.where(
                results[rural_col].fillna(0) > 0,
                (results[access_col].fillna(0) / results[rural_col].replace(0, np.nan) * 100).round(1),
                0,
            )
        else:
            context.info(f"  Skipping {ratio_col}: no rural denominator column '{rural_col}'")

    dep_num = results["rural_access_dependents"].fillna(0)
    dep_den = results["rural_access_working"].replace(0, pd.NA)
    results["rural_access_dependency_ratio"] = np.where(
        dep_den.notna() & (dep_den > 0),
        (dep_num / dep_den * 100).round(1),
        pd.NA,
    )

    # ----------------------------------------------------------------
    # 9. Select columns in desired order and save
    # ----------------------------------------------------------------
    final_cols = [id_col] + [c for c in RAI_OUTPUT_COLUMNS if c in results.columns]
    results[final_cols].round(1).to_csv(out_csv, index=False)
    context.info(f"RAI CSV written: {out_csv} ({len(final_cols)} cols)")

    shutil.rmtree(temp_dir, ignore_errors=True)

    return str(out_csv)
