import argparse
import os
import sys
import requests
import geopandas as gpd
import pandas as pd
import overpass
import warnings
from pathlib import Path

os.environ["OGR_GEOJSON_MAX_OBJ_SIZE"] = "0" # no limits when reading complex geojsons

warnings.simplefilter("ignore", UserWarning)

OVERPASS_FILTERS = {
    "education": ["nwr[amenity=school]"],
    "hospitals": ["nwr[amenity=hospital]","nwr[healthcare=hospital]"],
    
    "primary_healthcare": [
        'nwr["amenity"~"^(doctors|clinic)$"]["amenity"!="hospital"]["healthcare"!="hospital"]',
        'nwr["healthcare"~"^(clinic|doctors|midwife|nurse|center)$"]["amenity" != "hospital"]["healthcare" != "hospital"]'
    ] 
}

OHSOME_ENDPOINT = "https://api.ohsome.org/v1/elements/geometry"

OHSOME_FILTERS = {
    "education": "amenity=school",
    "hospitals": "amenity=hospital or healthcare=hospital",
    "primary_healthcare": (
        "not amenity=hospital and not healthcare=hospital and "
        "(amenity=doctors or amenity=clinic or healthcare=clinic or "
        "healthcare=doctors or healthcare=midwife or healthcare=nurse or healthcare=center)"
    ),
}



def parse_overpass_csv_to_gpd(result):
    headers = result[0]
    rows = result[1:]
    df = pd.DataFrame(rows, columns=headers)

    df['@lon'] = df['@lon'].astype(float)
    df['@lat'] = df['@lat'].astype(float)
    df['@id'] = df['@id'].astype(int)

    df = df.rename(columns={'@lon': 'lon', '@lat': 'lat', '@id': 'osmId'})

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df['lon'], df['lat']),
        crs="EPSG:4326"
    )
    return gdf

def fetch_overpass(context_log, boundary_file, output_dir, country_code, admin_level, time=None):
    context_log.info("Using Overpass API to fetch facilities...")
    id_col = f"{admin_level.upper()}_PCODE"

    # Paths for output files
    temp_dir = output_dir / "Temporary"
    out_dir = output_dir / "Output"
    expected_files = [
        out_dir / f"{country_code}_{admin_level}_facilities.csv"
    ] + [
        temp_dir / f"{country_code}_{category}_raw.geojson"
        for category in OVERPASS_FILTERS.keys()
    ]

    # Check if all expected files exist → skip
    if all(f.exists() for f in expected_files):
        context_log.info("All Overpass output files exist. Skipping fetch_overpass.")
        return expected_files[0]  # Return main summary path

    try:
        boundary = gpd.read_file(boundary_file)
    except Exception as e:
        context_log.info(f"Error reading boundary: {e}")
        sys.exit(1)

    if id_col not in boundary.columns:
        context_log.warning(f"Expected ID column {id_col} not found in {boundary_file}")
        return None

    minx, miny, maxx, maxy = boundary.total_bounds.tolist()
    bbox_str = f"{miny},{minx},{maxy},{maxx}"
    date_clause = f'[date:"{time}"]' if time else ''

    api = overpass.API(timeout=300)
    category_gdfs = {}

    for category, exprs in OVERPASS_FILTERS.items():
        filter_parts = [f"{expr}({bbox_str});" for expr in exprs]
        query = f'[out:csv(::lon,::lat,::id,::type)]{date_clause};({"".join(filter_parts)});out center;'

        try:
            result = api.get(query, build=False)
        except Exception as e:
            context_log.info(f"Overpass query failed for {category}: {e}")
            continue

        if not result or len(result) < 2:
            context_log.info(f"No results for {category}")
            continue

        gdf = parse_overpass_csv_to_gpd(result)

        if gdf.empty:
            context_log.info(f"No features found for {category}")
            continue

        gdf["geometry"] = gdf.geometry.centroid
        gdf["category"] = category

        raw_path = temp_dir / f"{country_code}_{category}_raw.geojson"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(raw_path, driver="GeoJSON")
        context_log.info(f"Wrote raw {category} features to {raw_path}")

        category_gdfs[category] = gdf

    if not category_gdfs:
        context_log.info("No Overpass features found at all.")
        return None

    counts = boundary[[id_col]].copy()
    for cat, gdf in category_gdfs.items():
        joined = gpd.sjoin(gdf, boundary, how="inner", predicate="intersects")
        grouped = joined.groupby(id_col).size().rename(f"{cat}_count")
        counts = counts.merge(grouped, on=id_col, how="left")

    counts = counts.fillna(0).astype({col: int for col in counts.columns if col != id_col})

    summary_path = out_dir / f"{country_code}_{admin_level}_facilities.csv"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    counts.to_csv(summary_path, index=False)
    context_log.info(f"Wrote summary to {summary_path}")

    return summary_path


def fetch_ohsome(context_log, boundary_file, output_dir, country_code, admin_level, time=None):
    context_log.info("Using Ohsome API to fetch facilities...")

    id_col = f"{admin_level.upper()}_PCODE"

    temp_dir = output_dir / "Temporary"
    out_dir = output_dir / "Output"
    expected_files = [
        out_dir / f"{country_code}_{admin_level}_facilities.csv"
    ] + [
        temp_dir / f"{country_code}_{category}_raw.geojson"
        for category in OHSOME_FILTERS.keys()
    ]

    if all(f.exists() for f in expected_files):
        context_log.info("All Ohsome output files exist. Skipping fetch_ohsome.")
        return expected_files[0]

    try:
        boundary = gpd.read_file(boundary_file)
    except Exception as e:
        context_log.info(f"Error reading boundary: {e}")
        sys.exit(1)

    if id_col not in boundary.columns:
        context_log.warning(f"Expected ID column {id_col} not found in {boundary_file}")
        return None

    minx, miny, maxx, maxy = boundary.total_bounds.tolist()
    bbox_str = f"{minx},{miny},{maxx},{maxy}"

    category_gdfs = {}

    for category, filter_str in OHSOME_FILTERS.items():
        params = {"bboxes": bbox_str, "filter": filter_str}
        if time:
            params["time"] = time

        try:
            r = requests.post(OHSOME_ENDPOINT, params=params)
            r.raise_for_status()
        except Exception as e:
            context_log.info(f"Ohsome query failed for {category}: {e}")
            continue

        data = r.json()
        gdf = gpd.GeoDataFrame.from_features(data["features"], crs="EPSG:4326")

        if gdf.empty:
            context_log.info(f"No features found for {category}")
            continue

        gdf = gdf.copy()
        gdf["geometry"] = gdf.geometry.centroid
        gdf["category"] = category

        raw_path = temp_dir / f"{country_code}_{category}_raw.geojson"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(raw_path, driver="GeoJSON")
        context_log.info(f"Wrote raw {category} features to {raw_path}")

        category_gdfs[category] = gdf

    if not category_gdfs:
        context_log.info("No categories found, nothing to aggregate.")
        return None

    counts = boundary[[id_col]].copy()
    for cat, gdf in category_gdfs.items():
        joined = gpd.sjoin(gdf, boundary, how="inner", predicate="intersects")
        grouped = joined.groupby(id_col).size().rename(f"{cat}_count")
        counts = counts.merge(grouped, on=id_col, how="left")

    counts = counts.fillna(0).astype({col: int for col in counts.columns if col != id_col})

    summary_path = out_dir / f"{country_code}_{admin_level}_facilities.csv"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    counts.to_csv(summary_path, index=False)
    context_log.info(f"Wrote summary to {summary_path}")

    return summary_path