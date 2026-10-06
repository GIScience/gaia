"""
Set the custom visualization (iframe) link on every existing HDX country page
to custom_viz_url(), without touching resources or other metadata and without
needing any local data. Countries without an HDX page are skipped.

Usage:
    python -m gaia.scripts.update_hdx_viz            # dry run, only reports
    python -m gaia.scripts.update_hdx_viz --apply    # writes to HDX
"""

import argparse
import sys

from dotenv import load_dotenv
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset

from gaia.scripts.check_hdx_pages import load_country_codes
from gaia.scripts.upload_to_hdx import (
    _hdx_config_from_env,
    custom_viz_url,
    get_dataset_hdx_name,
    get_hdx_country,
)


def update_hdx_viz(apply: bool) -> None:
    load_dotenv()
    hdx_config = _hdx_config_from_env()
    Configuration.create(
        hdx_site=hdx_config.site,
        user_agent="GaiaHdxVizUpdater",
        hdx_key=hdx_config.api_key,
    )

    updated, unchanged, failed = [], [], []
    for country_code in load_country_codes():
        dataset_hdx_name = get_dataset_hdx_name(get_hdx_country(country_code))
        dataset = Dataset.read_from_hdx(dataset_hdx_name)
        if not dataset:
            continue

        url = custom_viz_url(country_code)
        old = [v.get("url") for v in dataset.get("customviz") or []]
        if old == [url]:
            unchanged.append(country_code)
            continue

        print(f"[{country_code}] {old} -> {url}")
        if apply:
            try:
                dataset.set_custom_viz(url)
                dataset.update_in_hdx(update_resources=False)
            except Exception as e:
                print(f"[{country_code}] Update failed: {e}")
                failed.append(country_code)
                continue
        updated.append(country_code)

    verb = "Updated" if apply else "Would update"
    print(f"\n{verb} {len(updated)} page(s); {len(unchanged)} already up to date.")
    if failed:
        print(f"Failed: {failed}")
        sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Update the custom visualization link on all existing HDX pages."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the changes to HDX (default: dry run)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    update_hdx_viz(parse_args().apply)
