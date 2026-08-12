from pathlib import Path

import dagster as dg

from gaia.defs.constants import DroughtExposureConfig
from gaia.defs.partitions import country_partitions
from gaia.defs.utils import load_admin_boundary
from gaia.scripts.fetch_drought_exposure import calculate_drought_exposure


@dg.asset(
    deps=["demographics_asset", "facilities_asset"],
    partitions_def=country_partitions,
    ins={"boundary_asset": dg.AssetIn()},
)
def exposure_drought_asset(
    context, config: DroughtExposureConfig, boundary_asset: str
) -> list[str]:
    """
    Generate drought exposure CSVs using the JRC SPEI-6 global drought-class
    raster and WorldPop/facilities data. For each configured admin level,
    computes exposed populations and facilities per drought class.
    """
    country_code = context.partition_key.upper()
    base_path = Path(boundary_asset if boundary_asset else f"data/{country_code}")

    admin_levels = config.admin_levels
    if not admin_levels:
        raise ValueError("No admin_levels configured")

    outputs = []
    failures = []

    for admin_level in admin_levels:
        orig_level = admin_level
        level, boundary_path, _, _ = load_admin_boundary(
            base_path, country_code, admin_level
        )

        if not level:
            msg = f"No boundary found for {country_code} {orig_level} or lower levels"
            context.log.warning(msg)
            failures.append(msg)
            continue

        if level != orig_level:
            context.log.info(
                f"[{country_code}] Using fallback admin level {level} (requested {orig_level})"
            )

        admin_level = level  # use the found level

        context.log.info(
            f"Processing {country_code} {admin_level} using {boundary_path}"
        )

        csv_path = calculate_drought_exposure(
            context=context.log,
            country_code=country_code,
            admin_level=admin_level,
            api_choice=config.api.lower(),
        )
        if csv_path:
            outputs.append(csv_path)

    if not outputs and failures:
        failure_msg = f"Asset failed for {country_code}: " + "; ".join(failures)
        raise ValueError(failure_msg)

    if failures:
        context.log.warning(f"Some admin levels failed: {'; '.join(failures)}")

    return outputs
