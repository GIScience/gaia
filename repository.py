from dagster import Definitions
from assets.workflow_jobs import workflow_job, viz_job
# Import all assets
from assets.workflow_assets import (
    boundary_asset, 
    demographics_asset, 
    facilities_asset,
    exposure_flood_asset,
    exposure_cyclone_asset,
    rural_asset,
    access_asset,
    coping_asset,
    vulnerability_asset,
    prep_visualization_asset,
    risk_score_asset,
    upload_minio_asset,
    upload_viz_minio_asset,
    upload_hdx_asset,  
    check_hdx_downloads_asset,
    cleanup_asset,
)


# Assemble Definitions
defs = Definitions(
    assets=[
        boundary_asset,
        demographics_asset,
        facilities_asset,
        exposure_flood_asset,
        exposure_cyclone_asset,
        rural_asset,
        access_asset,
        coping_asset,
        vulnerability_asset,
        prep_visualization_asset,
        risk_score_asset,
        upload_minio_asset,
        upload_viz_minio_asset,
        upload_hdx_asset,
        check_hdx_downloads_asset,
        cleanup_asset,     
    ],
    jobs=[
        workflow_job,          # job without upload_hdx
        viz_job,
    ],
)