import os
from pathlib import Path

import dagster as dg

REPO_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = REPO_ROOT / "data"

# Defaults mirroring the former configs/assets_config.yaml
DEFAULT_ADMIN_LEVELS = ["ADM2"]
DEFAULT_RPS = ["10", "50", "100", "500"]
DEFAULT_FLOOD_THRESHOLD = 0.3  # meters
DEFAULT_FACILITIES_API = "ohsome-api" #or overpass
DEFAULT_CROPS_YEARS = [2023, 2024]
DEFAULT_NDVI_YEAR = [2022]
DEFAULT_ACLED_YEAR = 2021


class SetupConfig(dg.Config):
    admin_levels: list[str] = DEFAULT_ADMIN_LEVELS
    rps: list[str] = DEFAULT_RPS
    flood_threshold: float = DEFAULT_FLOOD_THRESHOLD


class FacilitiesConfig(dg.Config):
    api: str = DEFAULT_FACILITIES_API


class CropsConfig(dg.Config):
    years: list[int] = DEFAULT_CROPS_YEARS


class Ndviconfig(dg.Config):
    year: list[int] = DEFAULT_NDVI_YEAR


class AcledConfig(dg.Config):
    year: int = DEFAULT_ACLED_YEAR


# dagster only supports a single config parameter per asset (it must be named
# `config`). Assets that previously consumed multiple configs use a combined
# class so the pipeline keeps the same run-configurable fields.
class FacilitiesAssetConfig(SetupConfig, FacilitiesConfig):
    pass


class FloodExposureConfig(SetupConfig, FacilitiesConfig, CropsConfig):
    pass


class CycloneExposureConfig(SetupConfig, FacilitiesConfig):
    pass


class MinioConfig(dg.Config):
    endpoint: str = os.getenv("MINIO_ENDPOINT", "hot.storage.heigit.org")
    bucket: str = os.getenv("MINIO_BUCKET", "heigit-hdx-public")
    access_key: str = os.getenv("MINIO_ACCESS_KEY")
    secret_key: str = os.getenv("MINIO_SECRET_KEY")
    dest_prefix: str = os.getenv("MINIO_DEST_PREFIX", "risk_assessment_inputs")
    secure: bool = os.getenv("MINIO_SECURE", "true").lower() == "true"


class HdxConfig(dg.Config):
    site: str = os.getenv("HDX_SITE", "prod")
    api_key: str = os.getenv("HDX_API_KEY")
    owner_org: str = os.getenv(
        "HDX_OWNER_ORG", "heidelberg-institute-for-geoinformation-technology"
    )
    data_update_frequency: str = os.getenv(
        "HDX_DATA_UPDATE_FREQUENCY", "Every six months"
    )
    maintainer: str = os.getenv("HDX_MAINTAINER", "valentin-boehmer-8808")
    maintainer_email: str = os.getenv(
        "HDX_MAINTAINER_EMAIL", "valentin.boehmer@heigit.org"
    )
    private: bool = os.getenv("HDX_PRIVATE", "false").lower() == "true"
