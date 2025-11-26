# workflow_job.py

from dagster import define_asset_job
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
    #upload_minio_asset,
    # upload_hdx_asset  
)

# All assets you DO want in the job
ASSETS_TO_RUN = [
    boundary_asset,
    demographics_asset,
    facilities_asset,
    exposure_flood_asset,
    exposure_cyclone_asset,
    rural_asset,
    access_asset,
    coping_asset,
    vulnerability_asset,
    #upload_minio_asset,
]

workflow_job = define_asset_job(
    name="local_workflow_job",
    selection=ASSETS_TO_RUN,
)
