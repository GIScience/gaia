import os
from pathlib import Path

import dagster as dg

from gaia.defs.partitions import country_partitions
from gaia.defs.constants import (
    FloodExposureConfig,
    CycloneExposureConfig,
    CHUNK_MAX_CELLS,
    FLOOD_RES_DEG,
)
from gaia.defs.utils import load_admin_boundary, estimate_raster_cells
from gaia.scripts.fetch_floods_jrc import process_flood_impact, ALLOWED_RPS
from gaia.scripts.fetch_cyclones_ncei import calculate_cyclone_exposure


@dg.asset(
    deps=["demographics_asset", "facilities_asset"],
    partitions_def=country_partitions,
    ins={"boundary_asset": dg.AssetIn()},
)
def exposure_flood_asset(
    context,
    config: FloodExposureConfig,
    boundary_asset: str,
) -> list[str]:
    """
    For the given country, iterate over configured admin levels and RPs.
    Generate flooded population CSVs using GLOFAS and WorldPop.
    Skips missing admin levels or boundaries.
    Returns a list of output CSV paths.
    """
    country_code = context.partition_key.upper()
    base_path = Path(boundary_asset if boundary_asset else f"data/{country_code}")

    admin_levels = config.admin_levels
    if not admin_levels:
        raise ValueError("No admin_levels configured")

    rps = config.rps if config.rps else ALLOWED_RPS
    outputs = []

    for admin_level in admin_levels:
        orig_level = admin_level
        level, boundary_path, gdf, _ = load_admin_boundary(
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

        output_dir = base_path / "Output"
        os.makedirs(output_dir, exist_ok=True)

        # Recommend chunking when the flood raster footprint would exceed
        # CHUNK_MAX_CELLS. Processing is split per RP inside process_flood_impact.
        footprint_cells = estimate_raster_cells(gdf, FLOOD_RES_DEG)
        if footprint_cells > CHUNK_MAX_CELLS:
            context.log.info(
                f"[{country_code}] Flood raster footprint ~{footprint_cells:,} cells "
                f"exceeds CHUNK_MAX_CELLS ({CHUNK_MAX_CELLS:,}) → chunking recommended "
                "(flood processing will be split into spatial chunks)."
            )
        else:
            context.log.info(
                f"[{country_code}] Flood raster footprint ~{footprint_cells:,} cells "
                f"≤ CHUNK_MAX_CELLS ({CHUNK_MAX_CELLS:,}) → processing whole country."
            )

        csv_path = process_flood_impact(
            context=context.log,
            country_code=country_code,
            rps=rps,
            gdf=gdf,
            admin_level=admin_level,
            output_dir=str(output_dir),
            flood_threshold=config.flood_threshold,
            api_choice=config.api.lower(),
            crop_years=config.years,
            chunking=True,
            chunk_max_cells=CHUNK_MAX_CELLS,
            res_deg=FLOOD_RES_DEG,
        )
        outputs.append(csv_path)

    return outputs


@dg.asset(
    deps=["demographics_asset", "facilities_asset"],
    partitions_def=country_partitions,
    ins={"boundary_asset": dg.AssetIn()},
)
def exposure_cyclone_asset(
    context, config: CycloneExposureConfig, boundary_asset: str
) -> list[str]:
    """
    Generate cyclone exposure CSVs using IBTrACS data and WorldPop/facilities data.
    For each configured admin level, computes exposed populations and facilities.
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

        csv_path = calculate_cyclone_exposure(
            context=context.log,
            country_code=country_code,
            admin_level=admin_level,
            api_choice=config.api.lower(),
        )
        if csv_path:
            outputs.append(csv_path)
        else:
            # calculate_cyclone_exposure returns None only when there are
            # no cyclone tracks near the country or no buffers intersect
            # the country boundary — this is not an error, just a data gap.
            context.log.info(
                f"No cyclone data for {country_code} {admin_level} — "
                "no tracks intersect the country. Skipping."
            )

    if not outputs and failures:
        failure_msg = f"Asset failed for {country_code}: " + "; ".join(failures)
        raise ValueError(failure_msg)

    if failures:
        context.log.warning(f"Some admin levels failed: {'; '.join(failures)}")

    return outputs
