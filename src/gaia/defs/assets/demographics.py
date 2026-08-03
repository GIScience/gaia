import dagster as dg

from gaia.defs.partitions import country_partitions
from gaia.defs.constants import SetupConfig
from gaia.scripts.fetch_worldpop import aggregate_worldpop_to_csv


@dg.asset(
    deps=["boundary_asset"],
    partitions_def=country_partitions,
)
def demographics_asset(context, config: SetupConfig) -> list[str]:
    """
    For the given country (partition key), run WorldPop processing for each
    admin level specified in the setup config.
    Returns a list of output CSV paths.
    """
    country_code = context.partition_key.upper()

    admin_levels = config.admin_levels
    if not admin_levels:
        raise ValueError("No admin_levels configured")

    outputs = []

    for admin_level in admin_levels:
        orig_level = admin_level

        # Extract numeric (ADM2 → 2)
        lvl_num = int(admin_level.replace("ADM", ""))

        # Try ADM(level), ADM(level-1), ... ADM0
        fallback_levels = [f"ADM{n}" for n in range(lvl_num, -1, -1)]

        success = False

        for test_level in fallback_levels:
            try:
                output_csv = aggregate_worldpop_to_csv(
                    country_code=country_code,
                    admin_level=test_level,
                    context_log=context.log,
                )

                if test_level != orig_level:
                    context.log.info(
                        f"[{country_code}] WorldPop fallback: using {test_level} "
                        f"instead of requested {orig_level}"
                    )

                outputs.append(output_csv)
                success = True
                break  # exit fallback loop

            except Exception as e:
                context.log.warning(
                    f"[{country_code}] WorldPop failed for {test_level}: {e}"
                )
                continue

        if not success:
            context.log.warning(
                f"[{country_code}] No available admin level found for requested {orig_level} "
                f"(tried {fallback_levels})"
            )
            continue

    return outputs
