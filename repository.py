from dagster import Definitions
from assets.workflow_jobs import workflow_job
# Import all assets
from assets.workflow_assets import (
    boundary_asset, 
    demographics_asset, 
    facilities_asset,
    exposure_flood_asset,
    ndvi_asset,
    crops_asset,
    rural_asset,
    access_asset,
    coping_asset,
    vulnerability_asset,
    upload_minio_asset,
    upload_hdx_asset,  
)


# Assemble Definitions
defs = Definitions(
    assets=[
            boundary_asset, 
            demographics_asset, 
            upload_minio_asset,
            upload_hdx_asset,
            facilities_asset,
            exposure_flood_asset,
            ndvi_asset,
            crops_asset,
            rural_asset,
            vulnerability_asset,
            access_asset,
            coping_asset],
)