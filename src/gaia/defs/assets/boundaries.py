import os

import geopandas as gpd
import dagster as dg

from gaia.defs.partitions import country_partitions
from gaia.scripts.fetch_boundaries_hdx import download_shapefiles


@dg.asset(partitions_def=country_partitions)
def boundary_asset(context) -> str:
    """
    Downloads administrative boundary shapefiles from HDX and converts to GeoJSON.
    Checks which admin levels were generated and ensures the ID columns are correctly named.
    """
    country_code = context.partition_key.upper()
    download_shapefiles(country_code)

    data_dir = f"data/{country_code}"
    max_admin_level = 2  # ADM0 to ADM2
    found_cols = {}

    if os.path.exists(data_dir):
        for level in range(max_admin_level + 1):
            admin_file = os.path.join(data_dir, f"{country_code}_ADM{level}.geojson")
            expected_col = f"ADM{level}_PCODE"

            if os.path.exists(admin_file):
                gdf = gpd.read_file(admin_file)

                # If expected column exists, mark as found
                if expected_col in gdf.columns:
                    found_cols[f"ADM{level}"] = expected_col
                    continue

                # Fallback: search for a column that includes admin number and "code"
                fallback_col = None
                for col in gdf.columns:
                    col_lower = col.lower()
                    if str(level) in col_lower and (
                        "cod" in col_lower or "id" in col_lower
                    ):
                        fallback_col = col
                        break

                if fallback_col:
                    # Rename column
                    gdf = gdf.rename(columns={fallback_col: expected_col})
                    gdf.to_file(admin_file, driver="GeoJSON")
                    context.log.info(
                        f"{country_code}: Renamed column {fallback_col} -> {expected_col} in {admin_file}"
                    )
                    found_cols[f"ADM{level}"] = expected_col
                else:
                    context.log.warning(
                        f"{country_code}: No suitable ID column found for ADM{level} in {admin_file}"
                    )
            else:
                context.log.warning(
                    f"{country_code}: ADM{level} file not found ({admin_file})"
                )

    if found_cols:
        context.log.info(
            f"{country_code}: Detected ID columns per admin level: {found_cols}"
        )
    else:
        context.log.warning(
            f"{country_code}: No ID columns detected in any generated GeoJSON files"
        )

    return data_dir
