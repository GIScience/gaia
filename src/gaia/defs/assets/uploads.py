import os

import requests
import dagster as dg

from gaia.defs.partitions import country_partitions, multi_partitions
from gaia.defs.resources import S3Resource, HdxResource


@dg.asset(
    deps=[
        "demographics_asset",
        "facilities_asset",
        "coping_asset",
        "exposure_flood_asset",
        "exposure_cyclone_asset",
        "evacuability_asset",
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

    asset_filenames = {
        "demographics": f"{country_code}_ADM2_demographics.csv",
        "facilities": f"{country_code}_ADM2_facilities.csv",
        "flood_exposure": f"{country_code}_ADM2_flood_exposure.csv",
        "cyclone_exposure": f"{country_code}_ADM2_cyclone_exposure.csv",
        "rural_population": f"{country_code}_ADM2_rural_population.csv",
        "access": f"{country_code}_ADM2_access.csv",
        "coping": f"{country_code}_ADM2_coping.csv",
        "vulnerability": f"{country_code}_ADM2_vulnerability.csv",
    }

    file_map = {}
    base_output_dir = os.path.join("data", country_code, "Output")

    context.log.info(f"Scanning {base_output_dir} for indicator files...")

    for label, filename in asset_filenames.items():
        # Construct the manual path
        local_path = os.path.join(base_output_dir, filename)

        # Check if the file actually exists on the disk
        if os.path.exists(local_path):
            file_map[label] = local_path
            context.log.info(f"Found file for {label}: {filename}")
        else:
            context.log.warning(
                f"File not found for {label}: {filename}. Skipping from upload."
            )

    url = hdx.smart_upload(
        country_code=country_code,
        file_map=file_map,
        context=context,
    )

    return url


@dg.asset(
    deps=["upload_hdx_asset"],
    partitions_def=country_partitions,
)
def check_hdx_downloads_asset(context) -> bool:
    """
    Check that uploaded datasets are accessible on HDX (HOT storage public links).

    Rules:
    - If all expected files exist → success
    - If no files exist → success (country not on HDX)
    - If some files exist but at least one is missing → fail
    """
    country = context.partition_key.upper()

    FILE_TYPES = [
        "access",
        "coping",
        "demographics",
        "facilities",
        "flood_exposure",
        "rural_population",
        "vulnerability",
    ]

    ADM_LEVELS = ["ADM2", "ADM1"]

    BASE_HDX_URL = (
        "https://hot.storage.heigit.org/heigit-hdx-public/"
        "risk_assessment_inputs/{country}/{filename}"
    )

    missing_files = []
    existing_files = []

    for file_type in FILE_TYPES:
        file_found = False
        for adm in ADM_LEVELS:
            filename = f"{country}_{adm}_{file_type}.csv"
            url = BASE_HDX_URL.format(country=country.lower(), filename=filename)

            try:
                r = requests.head(url, timeout=30)
                if r.status_code == 200:
                    context.log.info(f"[{country}] HDX file accessible: {filename}")
                    existing_files.append(filename)
                    file_found = True
                    break  # stop at first available ADM level
                elif r.status_code == 404:
                    continue  # try next ADM level
                else:
                    context.log.warning(
                        f"[{country}] HDX file returned {r.status_code}: {filename}"
                    )
                    missing_files.append((filename, f"HTTP {r.status_code}"))
                    file_found = True
                    break
            except Exception as e:
                context.log.error(
                    f"[{country}] Error accessing HDX file {filename}: {e}"
                )
                missing_files.append((filename, str(e)))
                file_found = True
                break

        if not file_found:
            context.log.warning(
                f"[{country}] HDX file not found: {file_type} (tried ADM2 and ADM1)"
            )
            missing_files.append((f"{country}_ADM2_or_ADM1_{file_type}.csv", "missing"))

    if 0 < len(existing_files) < len(FILE_TYPES):
        # Some files exist but not all → fail
        error_msg = "\n".join([f"{fname}: {reason}" for fname, reason in missing_files])
        raise RuntimeError(
            f"[{country}] Some HDX files are missing or not accessible:\n{error_msg}"
        )

    # Otherwise:
    # - All files exist → success
    # - No files exist → success (country not on HDX)
    if len(existing_files) == 0:
        context.log.info(
            f"[{country}] No HDX files found, assuming country not uploaded → OK"
        )
    else:
        context.log.info(f"[{country}] All HDX files are accessible")

    return True
