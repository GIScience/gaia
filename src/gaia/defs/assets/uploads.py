import os

import requests
import dagster as dg

from gaia.defs.partitions import country_partitions, multi_partitions
from gaia.defs.resources import S3Resource, HdxResource

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
    s3.upload(country, category)
    context.log.info(f"[{country}] Uploaded {category} dataset(s) to S3 successfully.")


@dg.asset(
    partitions_def=country_partitions,
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
)
def upload_hdx_asset(context, hdx: HdxResource) -> str | None:
    country_code = context.partition_key.upper()

    file_map = {}
    base_output_dir = os.path.join("data", country_code, "Output")

    context.log.info(f"Scanning {base_output_dir} for indicator files...")

    for label in REQUIRED_INDICATOR_LABELS + OPTIONAL_INDICATOR_LABELS:
        for adm in ADM_LEVELS:
            filename = f"{country_code}_{adm}_{label}.csv"
            local_path = os.path.join(base_output_dir, filename)
            if os.path.exists(local_path):
                file_map[label] = local_path
                context.log.info(f"Found file for {label}: {filename}")
                break
        else:
            context.log.warning(
                f"File not found for {label}: tried ADM2 and ADM1. Skipping from upload."
            )

    missing_required = [
        label for label in REQUIRED_INDICATOR_LABELS if label not in file_map
    ]
    if missing_required:
        context.log.warning(
            f"[{country_code}] Missing required indicator file(s) for HDX upload: "
            f"{missing_required}. Skipping HDX page creation/update."
        )
        deleted = hdx.delete_dataset(country_code=country_code, context=context)
        if deleted:
            context.log.warning(
                f"[{country_code}] Removed existing HDX page since not all "
                "required files are available."
            )
        return None

    url = hdx.smart_upload(
        country_code=country_code,
        file_map=file_map,
        context=context,
    )

    return url


@dg.asset(
    ins={"upload_hdx_asset": dg.AssetIn()},
    partitions_def=country_partitions,
)
def check_hdx_downloads_asset(context, upload_hdx_asset: str | None) -> bool:
    """
    Check that the files referenced from the country's HDX page are actually
    reachable on public storage (HOT hot.storage.heigit.org).

    upload_hdx_asset is the dataset URL when a page was created/updated this
    run, or None when the upload was skipped (missing required files, or an
    incomplete page was deleted) — in that case there is nothing to check.

    Since upload_hdx_asset only uploads once every required file is present,
    a page existing implies every required file must be accessible; any
    single one missing here means the upload silently failed and is a hard
    failure. cyclone_exposure is optional and only checked informationally.
    """
    country = context.partition_key.upper()

    if not upload_hdx_asset:
        context.log.info(
            f"[{country}] No HDX page was created/updated this run, skipping accessibility check."
        )
        return True

    BASE_HDX_URL = (
        "https://hot.storage.heigit.org/heigit-hdx-public/"
        "risk_assessment_inputs/{country}/{filename}"
    )

    session = requests.Session()

    def resolve(file_type):
        """Return (filename, error) for the first ADM level found accessible,
        or (None, reason) if none was."""
        for adm in ADM_LEVELS:
            filename = f"{country}_{adm}_{file_type}.csv"
            url = BASE_HDX_URL.format(country=country.lower(), filename=filename)
            try:
                r = session.head(url, timeout=30)
            except Exception as e:
                return filename, str(e)
            if r.status_code == 200:
                return filename, None
            if r.status_code != 404:
                return filename, f"HTTP {r.status_code}"
        return f"{country}_ADM2_or_ADM1_{file_type}.csv", "missing"

    missing_required = []
    for file_type in REQUIRED_INDICATOR_LABELS:
        filename, error = resolve(file_type)
        if error:
            context.log.warning(
                f"[{country}] Required HDX file not accessible: {file_type} ({error})"
            )
            missing_required.append((filename, error))
        else:
            context.log.info(f"[{country}] HDX file accessible: {filename}")

    for file_type in OPTIONAL_INDICATOR_LABELS:
        filename, error = resolve(file_type)
        if error:
            context.log.info(
                f"[{country}] Optional HDX file not present: {file_type} ({error})"
            )
        else:
            context.log.info(f"[{country}] HDX file accessible: {filename}")

    if missing_required:
        error_msg = "\n".join(f"{fname}: {reason}" for fname, reason in missing_required)
        raise RuntimeError(
            f"[{country}] HDX page exists but required file(s) are missing or "
            f"not accessible:\n{error_msg}"
        )

    context.log.info(f"[{country}] All required HDX files are accessible")
    return True
