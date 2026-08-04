from pathlib import Path

import dagster as dg

from gaia.defs.partitions import country_partitions
from gaia.defs.constants import FacilitiesAssetConfig
from gaia.defs.utils import find_best_available_admin_level
from gaia.scripts.fetch_facilities_ohsome_overpass import fetch_ohsome, fetch_overpass


@dg.asset(
    partitions_def=country_partitions,
    ins={"boundary_asset": dg.AssetIn()},
    pool="ohsome",
)
def facilities_asset(
    context, config: FacilitiesAssetConfig, boundary_asset: str
) -> dg.Output[list[str]]:
    """
    Extract health and education facilities via Ohsome or Overpass.
    """
    country_code = context.partition_key.upper()
    base_path = Path(boundary_asset if boundary_asset else f"data/{country_code}")

    admin_levels = config.admin_levels
    if not admin_levels:
        raise ValueError("No admin_levels configured")

    summary_paths = []

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

        output_dir = Path("data") / country_code

        api_choice = config.api.lower()

        if api_choice == "ohsome-api":
            summary_path = fetch_ohsome(
                context.log, boundary_path, output_dir, country_code, admin_level
            )
        elif api_choice == "overpass":
            summary_path = fetch_overpass(
                context.log, boundary_path, output_dir, country_code, admin_level
            )
        elif api_choice == "ohsome-parquet":
            context.log.info("Not implemented yet: ohsome-parquet")
            continue
        else:
            context.log.warning(
                f"No valid API configured for facilities_asset (got '{api_choice}')"
            )
            continue

        if summary_path:
            summary_paths.append(str(summary_path))

    if not summary_paths:
        raise RuntimeError(f"No facility outputs created for {country_code}")

    return dg.Output(
        summary_paths,
        metadata={
            "country": country_code,
            "outputs": summary_paths,
        },
    )
