from pathlib import Path

import dagster as dg

from gaia.defs.partitions import country_partitions
from gaia.defs.constants import SetupConfig
from gaia.defs.utils import load_admin_boundary
from gaia.scripts.compute_rai import compute_rai, download_road_data


@dg.asset(
    partitions_def=country_partitions,
    ins={
        "boundary_asset": dg.AssetIn(),
        "demographics_asset": dg.AssetIn(),
        "rural_asset": dg.AssetIn(),
    },
)
def RAI_asset(
    context, config: SetupConfig, boundary_asset: str, demographics_asset, rural_asset
) -> list[str]:
    """
    Compute Rural Accessibility Index (RAI) — percentage of population in
    rural areas (GHS-SMOD classes 11/12/13) within 2 km of a paved road.
    """
    country_code = context.partition_key.upper()
    base_path = Path(boundary_asset if boundary_asset else f"data/{country_code}")

    admin_levels = config.admin_levels
    if not admin_levels:
        raise ValueError("No admin_levels configured")

    # Download road data to Temporary/rai_roads/
    road_download_dir = base_path / "Temporary" / "rai_roads"
    road_paths = download_road_data(country_code, str(road_download_dir), context.log)

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

        admin_level = level

        context.log.info(
            f"Processing {country_code} {admin_level} using {boundary_path}"
        )

        output_dir = base_path / "Output"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Find matching demographics CSV for this admin level
        demo_csv = None
        for csv_path in demographics_asset:
            if f"{country_code}_{admin_level}_demographics" in csv_path:
                demo_csv = csv_path
                break
        if not demo_csv:
            context.log.warning(
                f"No demographics CSV for {admin_level}, trying first available"
            )
            demo_csv = demographics_asset[0] if demographics_asset else None

        if not demo_csv:
            context.log.error(f"No demographics CSV available for {country_code}")
            continue

        # Find matching rural CSV for this admin level
        rural_csv = None
        for csv_path in rural_asset:
            if f"{country_code}_{admin_level}_rural_population" in csv_path:
                rural_csv = csv_path
                break
        if not rural_csv:
            context.log.warning(
                f"No rural CSV for {admin_level}, trying first available"
            )
            rural_csv = rural_asset[0] if rural_asset else None

        if not rural_csv:
            context.log.error(f"No rural CSV available for {country_code}")
            continue

        csv_path = compute_rai(
            country_code=country_code,
            admin_level=admin_level,
            gdf_admin=gdf,
            output_dir=str(output_dir),
            work_dir=str(base_path / "Temporary"),
            mapillary_path=road_paths.get("mapillary"),
            planet_path=road_paths.get("planet"),
            demographics_csv=demo_csv,
            rural_csv=rural_csv,
            context=context.log,
        )
        outputs.append(csv_path)

    return outputs
