import dagster as dg

from gaia.defs.partitions import country_partitions

local_workflow_job = dg.define_asset_job(
    name="local_workflow_job",
    description="Compute all GAIA risk assessment indicators for a country (no uploads).",
    partitions_def=country_partitions,
    config={
        "execution": {"config": {"max_concurrent": 1}},
        "retries": {"enabled": True, "max_retries": 2},
    },
    selection=[
        "boundary_asset",
        "demographics_asset",
        "facilities_asset",
        "exposure_flood_asset",
        "exposure_cyclone_asset",
        "evacuability_asset",
        "rural_asset",
        "RAI_asset",
        "access_asset",
        "coping_asset",
        "vulnerability_asset",
        "cleanup_asset",
    ],
)

visualization_job = dg.define_asset_job(
    name="visualization_job",
    description="Generate combined parquet, risk scores and PMTiles, and upload to S3.",
    selection=[
        "boundary_asset",
        "prep_visualization_asset",
        "risk_score_asset",
        "upload_viz_s3_asset",
    ],
)
