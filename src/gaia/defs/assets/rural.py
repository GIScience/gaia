import os
from pathlib import Path

import geopandas as gpd
import dagster as dg

from gaia.defs.partitions import country_partitions
from gaia.defs.constants import SetupConfig
from gaia.defs.utils import find_best_available_admin_level
from gaia.scripts.fetch_ruralness_ghsl import compute_rural_population


@dg.asset(
    partitions_def=country_partitions,
    ins={"boundary_asset": dg.AssetIn(), "demographics_asset": dg.AssetIn()},
)
def rural_asset(
    context, config: SetupConfig, boundary_asset: str, demographics_asset
) -> list[str]:
    """
    For the given country, iterate over configured admin levels.
    Generate rural population CSVs using WorldPop + SMOD.
    Skips missing admin levels or boundaries.
    Returns a list of output CSV paths.
    """
    country_code = context.partition_key.upper()
    base_path = Path(boundary_asset if boundary_asset else f"data/{country_code}")

    admin_levels = config.admin_levels
    if not admin_levels:
        raise ValueError("No admin_levels configured")

    outputs = []

    for admin_level in admin_levels:
        orig_level = admin_level
        level, boundary_path = find_best_available_admin_level(
            base_path, country_code, admin_level
        )

        if not level:
            context.log.warning(
                f"Skipping {country_code}: no boundary found for {orig_level} or lower levels"
            )
            continue

        if level != orig_level:
            context.log.info(
                f"[{country_code}] Using fallback admin level {level} (requested {orig_level})"
            )

        admin_level = level  # use the found level

        context.log.info(
            f"Processing {country_code} {admin_level} using {boundary_path}"
        )
        gdf = gpd.read_file(boundary_path)

        id_col = f"{admin_level.upper()}_PCODE"
        if id_col not in gdf.columns:
            context.log.warning(
                f"Skipping {country_code} {admin_level}: expected ID column '{id_col}' not found"
            )
            continue

        output_dir = base_path / "Output"
        os.makedirs(output_dir, exist_ok=True)

        csv_path = compute_rural_population(
            country_code=country_code,
            admin_level=admin_level,
            gdf=gdf,
            work_dir=str(Path()),
            output_dir=str(output_dir),
            context=context.log,
        )
        outputs.append(csv_path)

    return outputs
