import math
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

LAYER_PREFIXES = ["cop", "exp", "vul"]


def to_4326(gdf):
    """Ensure the GeoDataFrame is in EPSG:4326, reprojecting if needed."""
    if gdf.crs is None or gdf.crs.to_epsg() == 4326:
        return gdf
    return gdf.to_crs(epsg=4326)


def estimate_raster_cells(gdf, res_deg: float) -> int:
    """
    Approximate number of raster cells a dataset of resolution `res_deg`
    (degrees per pixel) would need to cover the GeoDataFrame's bounding box.
    Used to decide whether to chunk a country's processing (see CHUNK_MAX_CELLS).
    """
    xmin, ymin, xmax, ymax = gdf.total_bounds
    return math.ceil((xmax - xmin) / res_deg) * math.ceil((ymax - ymin) / res_deg)


def find_best_available_admin_level(
    base_path: Path, country_code: str, admin_level: str
):
    """
    Given the requested admin level (e.g. 'ADM2'), fallback to ADM1 → ADM0 when files are missing.
    Returns (final_level, path) or (None, None) if nothing exists.
    """
    lvl_num = int(admin_level.replace("ADM", ""))

    for test_lvl in range(lvl_num, -1, -1):  # e.g. 2 → 1 → 0
        level_name = f"ADM{test_lvl}"
        boundary_path = base_path / f"{country_code}_{level_name}.geojson"
        if boundary_path.exists():
            return level_name, boundary_path

    return None, None


def load_admin_boundary(base_path: Path, country_code: str, admin_level: str):
    """
    Resolve the requested admin level (falling back to lower levels when files
    are missing) and load the boundary with its expected *_PCODE column.

    Returns (level, boundary_path, gdf, id_col) or (None, None, None, None)
    when no boundary file with a usable ID column is available.
    """
    level, boundary_path = find_best_available_admin_level(
        base_path, country_code, admin_level
    )
    if not level:
        return None, None, None, None

    gdf = gpd.read_file(boundary_path)
    id_col = f"{level.upper()}_PCODE"
    if id_col not in gdf.columns:
        return None, None, None, None

    return level, boundary_path, gdf, id_col


def dedupe_adm_pcode(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse ADM*_PCODE merge artifacts into a single ADM_PCODE column."""
    adm_cols = [c for c in df.columns if c.startswith("ADM") and c.endswith("_PCODE")]
    if "ADM_PCODE_x" in df.columns or "ADM_PCODE_y" in df.columns:
        df["ADM_PCODE"] = df["ADM_PCODE_x"].combine_first(df["ADM_PCODE_y"])
        df.drop(
            columns=[c for c in ["ADM_PCODE_x", "ADM_PCODE_y"] if c in df.columns],
            inplace=True,
        )
    elif "ADM_PCODE" in df.columns and adm_cols.count("ADM_PCODE") > 1:
        df = df.loc[:, ~df.columns.duplicated()]
    return df


def normalize_indicators(indicators_df):
    def normalize(x):
        range_min = x.min()
        range_max = x.max()
        if pd.isna(range_min) or pd.isna(range_max) or range_max == range_min:
            return x
        return (x - range_min) / (range_max - range_min)

    for prefix in LAYER_PREFIXES:
        cols = [c for c in indicators_df.columns if c.startswith(prefix + "_")]
        indicators_df[cols] = indicators_df[cols].apply(normalize, axis=0)

    return indicators_df


def guess_missing_indicators(df):
    coping_columns = [c for c in df.columns if c.startswith("cop_")]
    df[coping_columns] = df[coping_columns].fillna(0)

    vulnerability_columns = [c for c in df.columns if c.startswith("vul_")]
    df[vulnerability_columns] = df[vulnerability_columns].fillna(1)

    return df


def calculate_geometric_mean(col1, col2):
    return np.sqrt(col1 * col2)
