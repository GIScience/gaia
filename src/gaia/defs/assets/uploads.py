import os
import re

import pandas as pd
import requests
import dagster as dg

from gaia.defs.partitions import country_partitions, multi_partitions
from gaia.defs.constants import HdxUploadConfig
from gaia.defs.resources import S3Resource, HdxResource
from gaia.defs.utils import check_ids_match_boundary

# Indicator files every HDX country page must have before it is created/updated.
REQUIRED_INDICATOR_LABELS = [
    "demographics",
    "facilities",
    "flood_exposure",
    "evacuability",
    "rural_population",
    "rai",
    "access",
    "coping",
    "vulnerability",
]
# Not every country has cyclone exposure data, so it's uploaded when present
# but never required.
OPTIONAL_INDICATOR_LABELS = ["cyclone_exposure"]
ADM_LEVELS = ["ADM2", "ADM1"]


@dg.asset(
    deps=[
        "demographics_asset",
        "facilities_asset",
        "exposure_flood_asset",
        "exposure_cyclone_asset",
        "evacuability_asset",
        "rural_asset",
        "access_asset",
        "coping_asset",
        "vulnerability_asset",
    ],
    partitions_def=multi_partitions,
)
def upload_s3_asset(context, s3: S3Resource) -> None:
    parts = context.partition_key.split("|")
    country, category = parts[1], parts[0]

    output_dir = os.path.join("data", country, "Output")

    if not os.path.isdir(output_dir):
        raise FileNotFoundError(f"[{country}] Output folder not found: {output_dir}")

    files = os.listdir(output_dir)
    matched = [f for f in files if category in f.lower()]

    if not matched:
        context.log.info(f"[{country}] No '{category}' outputs found in {output_dir}")
        return

    context.log.info(f"[{country}] Found {category} outputs: {matched}")

    # Indicators computed on an older boundary version must not reach S3/HDX
    for filename in matched:
        level = re.search(r"_(ADM\d)_", filename)
        if filename.endswith(".csv") and level:
            df = pd.read_csv(os.path.join(output_dir, filename))
            check_ids_match_boundary(
                df[f"{level.group(1)}_PCODE"], country, level.group(1), filename
            )

    s3.upload(country, category)
    context.log.info(f"[{country}] Uploaded {category} dataset(s) to S3 successfully.")


# Public S3 location of the indicator files linked from the HDX pages.
S3_PUBLIC_URL = (
    "https://hot.storage.heigit.org/heigit-hdx-public/"
    "risk_assessment_inputs/{country}/{filename}"
)


def exists_on_s3(session, url) -> bool:
    """True/False for HTTP 200/404. Raises on anything else, so an outage never
    counts as a missing file."""
    r = session.head(url, timeout=30)
    if r.status_code not in (200, 404):
        raise RuntimeError(f"HTTP {r.status_code} for {url}")
    return r.status_code == 200


@dg.asset(
    partitions_def=country_partitions,
    deps=["upload_s3_asset"],
)
def upload_hdx_asset(
    context, config: HdxUploadConfig, hdx: HdxResource
) -> str | None:
    """
    Creates/updates the country's HDX page from the indicator files on S3
    (local files aren't needed); see DEFAULT_HDX_UPLOAD_MODE for the modes.
    Never deletes a page, check_hdx_downloads_asset does that.
    """
    country_code = context.partition_key.upper()

    if config.mode == "metadata":
        return hdx.update_metadata(country_code=country_code, context=context)

    session = requests.Session()
    links = []
    missing_required = []
    for label in REQUIRED_INDICATOR_LABELS + OPTIONAL_INDICATOR_LABELS:
        for adm in ADM_LEVELS:
            filename = f"{country_code}_{adm}_{label}.csv"
            url = S3_PUBLIC_URL.format(country=country_code.lower(), filename=filename)
            if exists_on_s3(session, url):
                links.append((filename, url))
                context.log.info(f"[{country_code}] Found on S3: {filename}")
                break
        else:
            if label in REQUIRED_INDICATOR_LABELS:
                missing_required.append(label)

    if missing_required:
        context.log.warning(
            f"[{country_code}] Missing required indicator file(s) on S3: "
            f"{missing_required}. Skipping HDX page creation/update."
        )
        return None

    return hdx.smart_upload(country_code=country_code, links=links, context=context)


@dg.asset(
    deps=["upload_hdx_asset"],
    partitions_def=country_partitions,
)
def check_hdx_downloads_asset(context, hdx: HdxResource) -> bool:
    """
    Deletes the country's HDX page if it doesn't list every required
    indicator file, or if one of them isn't reachable on public storage
    (HOT hot.storage.heigit.org). Returns False when the page was deleted.
    cyclone_exposure is optional and only checked informationally.
    """
    country = context.partition_key.upper()

    dataset = hdx.get_dataset(country)
    if not dataset:
        context.log.info(f"[{country}] No HDX page, nothing to check.")
        return True

    resources = {res.get("name"): res.get("url") for res in dataset.get_resources()}
    session = requests.Session()

    def listed_file(file_type):
        """Name of the page's resource for `file_type`, or None if not listed."""
        names = [f"{country}_{adm}_{file_type}.csv" for adm in ADM_LEVELS]
        return next((name for name in names if name in resources), None)

    problems = []
    for file_type in REQUIRED_INDICATOR_LABELS:
        name = listed_file(file_type)
        if not name:
            problems.append(f"{file_type}: not listed on the page")
        elif not exists_on_s3(session, resources[name]):
            problems.append(f"{name}: not accessible on S3")
        else:
            context.log.info(f"[{country}] HDX file accessible: {name}")

    for file_type in OPTIONAL_INDICATOR_LABELS:
        name = listed_file(file_type)
        if name and not exists_on_s3(session, resources[name]):
            context.log.warning(f"[{country}] Optional HDX file not accessible: {name}")

    if problems:
        context.log.warning(
            f"[{country}] Deleting HDX page, required file(s) missing:\n"
            + "\n".join(problems)
        )
        dataset.delete_from_hdx()
        return False

    context.log.info(f"[{country}] All required HDX files are accessible")
    return True
