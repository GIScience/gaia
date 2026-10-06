import os
from pathlib import Path
from typing import Literal

import dagster as dg

REPO_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = REPO_ROOT / "data"

# Defaults mirroring the former configs/assets_config.yaml
DEFAULT_ADMIN_LEVELS = ["ADM2"]
# The only return periods JRC GLOFAS actually publishes flood-hazard tiles
# for; also used by fetch_floods_jrc.py to validate a requested RP.
DEFAULT_RPS = ["10", "50", "100", "500"]
DEFAULT_FLOOD_THRESHOLD = 0.3  # meters
DEFAULT_FACILITIES_API = "ohsome-api"  # or overpass
# ESA WorldCover ships exactly two map releases (2020, 2021) rather than an
# annual product (see fetch_worldcover.WORLDCOVER_VERSIONS); a requested year
# outside that set snaps back to this default.
DEFAULT_CROPS_YEAR = 2021
# Kept as a list for config-schema continuity with CropsConfig.years below;
# only the last entry is actually used (see fetch_worldcover._resolve_year).
DEFAULT_CROPS_YEARS = [DEFAULT_CROPS_YEAR]
# SPEI-6 <= this value counts as a drought month ("severe-or-worse"); -1.0
# would be "moderate-or-worse". See
# https://drought.emergency.copernicus.eu/tumbo/gdo/download/
DEFAULT_SPEI_THRESHOLD = -1.5
# A pixel-month only counts toward a drought event once this many
# consecutive months are all below the SPEI threshold.
DEFAULT_MIN_CONSECUTIVE_MONTHS = 3

# EU countries (`boundary_source: nuts` in countries.yaml) take their
# boundaries from Eurostat GISCO NUTS instead of OCHA COD-AB. Admin level ->
# NUTS level; ADM0 is dissolved from ADM1. NUTS3 is the closest match to the
# OCHA ADM2 level across the EU; for ADM1 a country can override the default
# with `nuts_adm1_level` in countries.yaml (e.g. 1 for the German Länder).
NUTS_YEAR = 2024
NUTS_LEVELS = {"ADM1": 2, "ADM2": 3}
GISCO_NUTS_URL = "https://gisco-services.ec.europa.eu/distribution/v2/nuts/geojson"
# French outermost regions (Guadeloupe, Martinique, Guyane, Réunion, Mayotte)
# have their own ISO3 codes (e.g. in WorldPop), so they're kept out of FRA.
NUTS_EXCLUDED_PREFIXES = ("FRY",)
# Copyright notice required by GISCO; written into the PMTiles metadata
# (`attribution`) so the frontend can display it.
NUTS_ATTRIBUTION = (
    '<a href="https://ec.europa.eu/eurostat/web/gisco/geodata/statistical-units/'
    'territorial-units-statistics">Eurostat GISCO NUTS 2024</a>, '
    "© EuroGeographics for the administrative boundaries"
)

# Shared across flood, cyclone, and drought exposure scripts.
FACILITY_CATEGORIES = ["education", "hospitals", "primary_healthcare"]
POP_INDICATORS = [
    "total_pop",
    "female_pop",
    "children_u5",
    "female_u5",
    "elderly",
    "pop_u15",
    "female_u15",
    "wra_pop",
    "dep_dependents",
    "dep_working",
]

# Risk score methodology, kept in sync with the Disaster Risk Composer
# dashboard (GIScience/Hazard-Risk-Composer), which recomputes the scores in
# the browser from the indicator columns of the *_risk.parquet files.
#
# Coping indicators are inverted (1 - normalized value, higher raw value =
# better coping) except those that already measure a lack of coping capacity;
# a cop_ column matching any of these regexes is used as-is.
COPING_NON_INVERTED_PATTERNS = (
    r"_evac_time_minutes_(mean|median|max)$",
    r"_pixels_at_risk$",
    r"_dependency_ratio$",
)
# Hazard-specific coping columns; every other cop_ column counts for all hazards.
COPING_HAZARD_PATTERNS = {
    "flood": r"^cop_RP\d+_",
    "cyclone": r"^cop_kt34_",
}
# Exposure column prefix per hazard scored in the *_risk.parquet.
EXPOSURE_PREFIXES = {
    "flood": "exp_flo_",
    "cyclone": "exp_cyc_",
}
# Indicator columns kept out of the *_risk.parquet: the dashboard has no
# drought hazard yet and would treat exp_dro_* / exp_drought as flood and
# cyclone inputs. Drought exposure stays in the local *_combined.parquet.
UNPUBLISHED_INDICATOR_PREFIXES = ("exp_dro_",)

# What upload_hdx_asset does with a country's HDX page. Works from the files
# on S3 only, never deletes a page (check_hdx_downloads_asset does that), and
# can be overridden per run (ops: upload_hdx_asset: config: mode: metadata).
#   "sync":     create/update the page once all required indicator files are
#               on S3; resources no longer on S3 are removed from the page.
#   "metadata": update everything except the resources, on existing pages only.
DEFAULT_HDX_UPLOAD_MODE = "sync"

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


# dagster only supports a single config parameter per asset (it must be named
# `config`). Assets that previously consumed multiple configs use a combined
# class so the pipeline keeps the same run-configurable fields.
class FacilitiesAssetConfig(SetupConfig, FacilitiesConfig):
    pass


class FloodExposureConfig(SetupConfig, FacilitiesConfig, CropsConfig):
    pass


class CycloneExposureConfig(SetupConfig, FacilitiesConfig, CropsConfig):
    pass


class DroughtExposureConfig(FacilitiesConfig, CropsConfig):
    admin_levels: list[str] = DEFAULT_ADMIN_LEVELS
    spei_threshold: float = DEFAULT_SPEI_THRESHOLD
    min_consecutive_months: int = DEFAULT_MIN_CONSECUTIVE_MONTHS


class HdxUploadConfig(dg.Config):
    mode: Literal["sync", "metadata"] = DEFAULT_HDX_UPLOAD_MODE
