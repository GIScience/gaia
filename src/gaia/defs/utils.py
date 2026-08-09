import math
from pathlib import Path

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
