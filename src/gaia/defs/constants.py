import os
from pathlib import Path

import dagster as dg

REPO_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = REPO_ROOT / "data"

# Defaults mirroring the former configs/assets_config.yaml
DEFAULT_ADMIN_LEVELS = ["ADM2"]
DEFAULT_RPS = ["10", "50", "100", "500"]
DEFAULT_FLOOD_THRESHOLD = 0.3  # meters
DEFAULT_FACILITIES_API = "ohsome-api"  # or overpass
DEFAULT_CROPS_YEARS = [2023, 2024]
DEFAULT_NDVI_YEAR = [2022]
DEFAULT_ACLED_YEAR = 2021

# Flood chunking: when a country's ADM2 raster footprint exceeds this many
# cells, exposure_flood_asset splits the country into smaller chunks (groups of
# ADM2 units) so each run only keeps a bounded raster in memory. Tune per
# machine RAM — CHUNK_MAX_CELLS float32 ≈ CHUNK_MAX_CELLS * 4 bytes.
CHUNK_MAX_CELLS = int(os.getenv("GAIA_CHUNK_MAX_CELLS", "200000000"))
# Approximate ground resolution of the GLOFAS flood depth tiles (~100 m).
FLOOD_RES_DEG = float(os.getenv("GAIA_FLOOD_RES_DEG", str(1 / 1200)))


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


class S3Config(dg.Config):
    endpoint: str = os.getenv("S3_ENDPOINT", "hot.storage.heigit.org")
    bucket: str = os.getenv("S3_BUCKET", "heigit-hdx-public")
    access_key: str = os.getenv("S3_ACCESS_KEY")
    secret_key: str = os.getenv("S3_SECRET_KEY")
    dest_prefix: str = os.getenv("S3_DEST_PREFIX", "risk_assessment_inputs")
    secure: bool = os.getenv("S3_SECURE", "true").lower() == "true"


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
