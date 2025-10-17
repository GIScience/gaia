from dagster import Definitions
from assets.workflow_assets import (
    boundary_asset, 
    demographics_asset, 
    upload_minio_asset, 
    upload_hdx_asset, 
    facilities_asset, 
    exposure_flood_asset,
    rural_asset,
    vulnerability_asset,
    access_asset,
    coping_asset
)

defs = Definitions(
    assets=[
            boundary_asset, 
            demographics_asset, 
            upload_minio_asset,
            upload_hdx_asset,
            facilities_asset,
            exposure_flood_asset,
            rural_asset,
            vulnerability_asset,
            access_asset,
            coping_asset],
)