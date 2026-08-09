import argparse
import io
import os
import sys
import time
import requests
import geopandas as gpd
import pandas as pd
import overpass
import warnings
from datetime import datetime, timezone
from pathlib import Path
from shapely.geometry import mapping

from gaia.defs.utils import to_4326

os.environ["OGR_GEOJSON_MAX_OBJ_SIZE"] = "0"  # no limits when reading complex geojsons

warnings.simplefilter("ignore", UserWarning)

OVERPASS_FILTERS = {
    "education": ["nwr[amenity=school]"],
    "hospitals": ["nwr[amenity=hospital]", "nwr[healthcare=hospital]"],
    "primary_healthcare": [
        'nwr["amenity"~"^(doctors|clinic)$"]["amenity"!="hospital"]["healthcare"!="hospital"]',
        'nwr["healthcare"~"^(clinic|doctors|midwife|nurse|center)$"]["amenity" != "hospital"]["healthcare" != "hospital"]',
    ],
}

OHSOME_BASE_URL = "https://api.heigit.org/ohsome-api-staging/v2"
OHSOME_COUNT_ENDPOINT = f"{OHSOME_BASE_URL}/stats/features/count.json"
OHSOME_EXTRACTION_ENDPOINT = f"{OHSOME_BASE_URL}/extraction/features.parquet"
OHSOME_API_KEY = os.getenv("OHSOME_API_KEY", "")
OHSOME_TIME_SERIES_START = "2020-01-01T00:00:00Z"
OHSOME_TIME_SERIES_INTERVAL = "P1Y"
OHSOME_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
OHSOME_MAX_RETRIES = 4
OHSOME_RETRY_BASE_DELAY = 2.0
# The ohsome count endpoint rejects requests whose aoi geometry is too large
# with HTTP 413 (very complex admin units, e.g. Chile's fjord coastlines).
# On 413 the geometry is progressively simplified with these tolerances
# (in degrees) until the request fits. 0.0 = original geometry, tried first.
OHSOME_GEOM_SIMPLIFY_TOLERANCES = (0.0, 0.001, 0.005, 0.01, 0.05)

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

    df["@lon"] = df["@lon"].astype(float)
    df["@lat"] = df["@lat"].astype(float)
    df["@id"] = df["@id"].astype(int)

    df = df.rename(columns={"@lon": "lon", "@lat": "lat", "@id": "osmId"})

    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df["lon"], df["lat"]), crs="EPSG:4326"
    )
    return gdf


def fetch_overpass(
    context_log, boundary_file, output_dir, country_code, admin_level, time=None
):
    context_log.info("Using Overpass API to fetch facilities...")
    id_col = f"{admin_level.upper()}_PCODE"

    # Paths for output files
    temp_dir = output_dir / "Temporary"
    out_dir = output_dir / "Output"
    expected_files = [out_dir / f"{country_code}_{admin_level}_facilities.csv"] + [
        temp_dir / f"{country_code}_{category}_raw.geojson"
        for category in OVERPASS_FILTERS.keys()
    ]

    # Check if all expected files exist → skip
    if all(f.exists() for f in expected_files):
        context_log.info("All Overpass output files exist. Skipping fetch_overpass.")
        return expected_files[0]  # Return main summary path

    try:
        boundary = to_4326(gpd.read_file(boundary_file))
    except Exception as e:
        context_log.info(f"Error reading boundary: {e}")
        sys.exit(1)

    if id_col not in boundary.columns:
        context_log.warning(f"Expected ID column {id_col} not found in {boundary_file}")
        return None

    minx, miny, maxx, maxy = boundary.total_bounds.tolist()
    bbox_str = f"{miny},{minx},{maxy},{maxx}"
    date_clause = f'[date:"{time}"]' if time else ""

    api = overpass.API(timeout=300)
    category_gdfs = {}

    for category, exprs in OVERPASS_FILTERS.items():
        filter_parts = [f"{expr}({bbox_str});" for expr in exprs]
        query = f"[out:csv(::lon,::lat,::id,::type)]{date_clause};({''.join(filter_parts)});out center;"

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

    counts = counts.fillna(0).astype(
        {col: int for col in counts.columns if col != id_col}
    )

    summary_path = out_dir / f"{country_code}_{admin_level}_facilities.csv"
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    # Add ADM_PCODE duplicate for consistency
    if "ADM_PCODE" not in counts.columns and id_col in counts.columns:
        counts["ADM_PCODE"] = counts[id_col]

    # Reorder columns so ADM_PCODE comes right after the main ID
    cols = [id_col, "ADM_PCODE"] + [
        c for c in counts.columns if c not in [id_col, "ADM_PCODE"]
    ]
    counts = counts[cols]

    counts.to_csv(summary_path, index=False)
    context_log.info(f"Wrote summary to {summary_path}")

    return summary_path


def _post_with_retries(url, headers, body, context_log):
    """POST with exponential backoff on transient HTTP errors (429/5xx).

    The staging API is occasionally unavailable; short retries with backoff
    keep a long per-admin-unit loop from failing outright on a flaky server.
    After the final attempt the request fails loudly instead of returning a
    failed response, so incomplete data is never written silently.
    """
    delay = OHSOME_RETRY_BASE_DELAY
    for attempt in range(OHSOME_MAX_RETRIES + 1):
        try:
            r = requests.post(url, headers=headers, json=body, timeout=180)
        except requests.RequestException as e:
            context_log.info(f"Ohsome request error (attempt {attempt + 1}): {e}")
            if attempt == OHSOME_MAX_RETRIES:
                raise
            time.sleep(delay)
            delay *= 2
            continue

        if r.status_code not in OHSOME_RETRYABLE_STATUS:
            return r

        if attempt == OHSOME_MAX_RETRIES:
            raise RuntimeError(
                f"Ohsome request failed after {OHSOME_MAX_RETRIES + 1} attempts: "
                f"HTTP {r.status_code} from {url}"
            )

        wait = delay
        retry_after = r.headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            wait = max(wait, float(retry_after))
        context_log.info(
            f"Ohsome returned HTTP {r.status_code} (attempt {attempt + 1}); "
            f"retrying in {wait:.0f}s"
        )
        time.sleep(wait)
        delay *= 2

    raise RuntimeError(f"Ohsome request to {url} failed unexpectedly")


def _extract_raw_geometries(context_log, boundary, output_dir, country_code, time=None):
    """Fetch facility geometries per category via the ohsome extraction endpoint.

    The stats API only returns aggregated counts; the raw geometries that the
    flood/cyclone/drought steps sample rasters at come from the extraction
    endpoint. One request per category over the country bbox is enough.
    """
    minx, miny, maxx, maxy = boundary.total_bounds.tolist()
    aoi = [minx, miny, maxx, maxy]
    timestamp = time or "latest"

    temp_dir = output_dir / "Temporary"
    headers = {}
    if OHSOME_API_KEY:
        headers["Authorization"] = OHSOME_API_KEY

    for category, filter_str in OHSOME_FILTERS.items():
        body = {
            "clip": True,
            "timestamp": timestamp,
            "filter": filter_str,
            "aoi": aoi,
        }

        r = _post_with_retries(OHSOME_EXTRACTION_ENDPOINT, headers, body, context_log)
        r.raise_for_status()
        gdf = gpd.read_parquet(io.BytesIO(r.content))

        raw_path = temp_dir / f"{country_code}_{category}_raw.geojson"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(raw_path, driver="GeoJSON")
        context_log.info(f"Wrote raw {category} features to {raw_path}")


def _extract_count(data):
    """
    Extract the most recent feature count from the ohsome stats response.

    The endpoint returns a single time series as parallel arrays:
        {"result": {"timestamp": [...], "value": [...]}}
    The value at the latest timestamp is the count we want. A few other
    plausible shapes are kept as fallbacks for forward compatibility.
    """
    if not isinstance(data, dict):
        return None

    result = data.get("result")
    if isinstance(result, dict):
        values = result.get("value")
        if isinstance(values, list) and values:
            last = values[-1]
            if isinstance(last, (int, float)):
                return last
        for value_key in ("value", "count"):
            if value_key in result and isinstance(result[value_key], (int, float)):
                return result[value_key]

    for key in ("results", "data"):
        entries = data.get(key)
        if isinstance(entries, list) and entries:
            for entry in reversed(entries):
                if isinstance(entry, dict):
                    for value_key in ("value", "count"):
                        if value_key in entry and isinstance(
                            entry[value_key], (int, float)
                        ):
                            return entry[value_key]

    for key in ("value", "count"):
        if key in data and isinstance(data[key], (int, float)):
            return data[key]

    return None


def _query_ohsome_count(filter_str, geometry, end, context_log):
    """Query the new ohsome stats endpoint for one category in one aoi.

    `geometry` is the admin unit's shapely geometry. The full geometry can
    exceed the API's request size limit for very complex boundaries, which
    the server answers with HTTP 413. In that case the geometry is
    progressively simplified until the request fits, so the unit still gets
    a count instead of failing the whole asset.
    """
    headers = {}
    if OHSOME_API_KEY:
        headers["Authorization"] = OHSOME_API_KEY

    last_r = None
    for tolerance in OHSOME_GEOM_SIMPLIFY_TOLERANCES:
        aoi = (
            mapping(geometry)
            if tolerance == 0
            else mapping(geometry.simplify(tolerance, preserve_topology=True))
        )
        body = {
            "groupBy": None,
            "timeSeries": {
                "start": OHSOME_TIME_SERIES_START,
                "end": end,
                "interval": OHSOME_TIME_SERIES_INTERVAL,
            },
            "filter": filter_str,
            "aoi": aoi,
        }

        r = _post_with_retries(OHSOME_COUNT_ENDPOINT, headers, body, context_log)
        if r.status_code == 413:
            last_r = r
            if tolerance == 0:
                context_log.warning(
                    f"Ohsome count for filter '{filter_str}' too large (HTTP 413); "
                    "retrying with simplified geometry"
                )
            else:
                context_log.warning(
                    f"Ohsome count for filter '{filter_str}' still too large "
                    f"(HTTP 413) at tolerance {tolerance}°; retrying coarser"
                )
            continue

        r.raise_for_status()
        data = r.json()
        value = _extract_count(data)
        if value is None:
            context_log.warning(
                f"Ohsome response missing count for filter '{filter_str}': {data}"
            )
        return value

    raise RuntimeError(
        f"Ohsome count request for filter '{filter_str}' is too large even "
        f"after geometry simplification (last HTTP {last_r.status_code})"
    )


def fetch_ohsome(
    context_log, boundary_file, output_dir, country_code, admin_level, time=None
):
    context_log.info("Using Ohsome API (stats/features) to fetch facilities...")

    id_col = f"{admin_level.upper()}_PCODE"

    out_dir = output_dir / "Output"
    summary_path = out_dir / f"{country_code}_{admin_level}_facilities.csv"

    temp_dir = output_dir / "Temporary"
    raw_paths = [
        temp_dir / f"{country_code}_{category}_raw.geojson"
        for category in OHSOME_FILTERS
    ]

    summary_exists = summary_path.exists()
    raw_all_exist = all(p.exists() for p in raw_paths)

    if summary_exists and raw_all_exist:
        context_log.info(
            "Facilities summary and raw geometries exist. Skipping fetch_ohsome."
        )
        return summary_path

    try:
        boundary = to_4326(gpd.read_file(boundary_file))
    except Exception as e:
        context_log.info(f"Error reading boundary: {e}")
        return None

    if id_col not in boundary.columns:
        context_log.warning(f"Expected ID column {id_col} not found in {boundary_file}")
        return None

    if not raw_all_exist:
        _extract_raw_geometries(
            context_log, boundary, output_dir, country_code, time=time
        )

    if summary_exists:
        return summary_path

    end = time or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Query one admin unit at a time, passing the unit's polygon geometry as
    # the aoi so facilities are only counted inside the actual boundary.
    # (A bounding-box aoi would over-attribute boundary-adjacent facilities
    # to neighbouring units.)
    rows = []
    for _, row in boundary.iterrows():
        geometry = row.geometry

        row_data = {id_col: row[id_col], "ADM_PCODE": row[id_col]}
        for category, filter_str in OHSOME_FILTERS.items():
            count = _query_ohsome_count(filter_str, geometry, end, context_log)
            row_data[f"{category}_count"] = count if count is not None else 0
        rows.append(row_data)

    counts = pd.DataFrame(rows)

    count_cols = [c for c in counts.columns if c.endswith("_count")]
    counts[count_cols] = counts[count_cols].astype(int)

    # Reorder columns so ADM_PCODE comes right after the main ID
    cols = [id_col, "ADM_PCODE"] + count_cols
    counts = counts[cols]

    summary_path.parent.mkdir(parents=True, exist_ok=True)
    counts.to_csv(summary_path, index=False)
    context_log.info(f"Wrote summary to {summary_path}")

    return summary_path
