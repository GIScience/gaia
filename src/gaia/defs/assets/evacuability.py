import dagster as dg

from gaia.defs.partitions import country_partitions
from gaia.defs.constants import SetupConfig
from gaia.scripts.calculate_evacuatability import compute_evacuability_csv


@dg.asset(
    deps=["exposure_flood_asset", "exposure_cyclone_asset"],
    partitions_def=country_partitions,
)
def evacuability_asset(context, config: SetupConfig) -> list[str]:
    """
    Compute evacuability (travel time to safe zones) for flood and cyclone exposure.
    Reads the hazard rasters produced by exposure_flood_asset and exposure_cyclone_asset,
    calculates MCP-based travel times, and writes a standalone CSV with evacuability
    columns.

    Output: data/{country_code}/Output/{country_code}_{admin_level}_evacuability.csv

    Returns a list of paths to the evacuability CSVs.
    """
    country_code = context.partition_key.upper()
    admin_levels = config.admin_levels
    rps = config.rps

    if not admin_levels:
        raise ValueError("No admin_levels configured")

    outputs = []

    for admin_level in admin_levels:
        csv_path = compute_evacuability_csv(
            context=context.log,
            country_code=country_code,
            admin_level=admin_level,
            rps=rps,
            flood_threshold=config.flood_threshold,
        )
        if csv_path:
            outputs.append(csv_path)
            context.log.info(
                f"[{country_code}] Evacuability CSV produced for {admin_level}: {csv_path}"
            )
        else:
            context.log.warning(
                f"[{country_code}] Evacuability CSV skipped for {admin_level}"
            )

    if not outputs:
        context.log.warning(f"[{country_code}] No evacuability outputs produced")

    return outputs
