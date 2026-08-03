import shutil
from pathlib import Path

import dagster as dg

from gaia.defs.partitions import country_partitions


@dg.asset(
    deps=[
        "boundary_asset",
        "demographics_asset",
        "facilities_asset",
        "exposure_flood_asset",
        "exposure_cyclone_asset",
        "rural_asset",
        "access_asset",
        "coping_asset",
        "vulnerability_asset",
        "RAI_asset",
    ],
    partitions_def=country_partitions,
)
def cleanup_asset(context):
    """
    Deletes ALL intermediate files for the given country,
    keeping ONLY the Output/ folder.

    Structure kept:
        data/<COUNTRY>/
            Output/
                final CSVs ...

    Everything else is deleted:
        Temporary/
        *.tif, *.gpkg, *.zip
        intermediate *_pop.csv
        ohsome/parquet temp files
        isochrone caching
        any other intermediate folders
    """
    country_code = context.partition_key.upper()
    base_dir = Path("data") / country_code

    if not base_dir.exists():
        context.log.warning(f"[{country_code}] No data directory to clean.")
        return

    output_dir = base_dir / "Output"

    # --------------------------------------------------------
    # Step 1: Collect what should be preserved
    # --------------------------------------------------------
    preserved = set()

    # Always preserve Output/
    if output_dir.exists():
        preserved.add(output_dir.resolve())

        # preserve contents of Output/
        for p in output_dir.rglob("*"):
            preserved.add(p.resolve())

    # --------------------------------------------------------
    # Step 2: Delete all other files/folders in data/<COUNTRY>
    # --------------------------------------------------------
    for item in base_dir.iterdir():
        item_resolved = item.resolve()

        if item_resolved in preserved:
            continue  # skip Output/

        if item.is_dir():
            # delete folder except Output
            try:
                shutil.rmtree(item)
                context.log.info(f"[{country_code}] Removed folder: {item}")
            except Exception as e:
                context.log.warning(f"[{country_code}] Failed removing {item}: {e}")
        else:
            # delete file
            try:
                item.unlink()
                context.log.info(f"[{country_code}] Removed file: {item}")
            except Exception as e:
                context.log.warning(f"[{country_code}] Failed removing {item}: {e}")

    context.log.info(f"[{country_code}] Cleanup complete.")
