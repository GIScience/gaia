"""
Check, for every country listed in hdx_countries.yaml, whether it currently
has a live HDX dataset page ("Risk Assessment Indicators"), and write the
result to a YAML file.

Usage:
    python -m gaia.scripts.check_hdx_pages [--output existing_hdx_pages.yaml]
"""

import argparse
import sys
from importlib.resources import files

import yaml
from dotenv import load_dotenv
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset

from gaia.scripts.upload_to_hdx import (
    _hdx_config_from_env,
    get_dataset_hdx_name,
    get_hdx_country,
)


def load_country_codes() -> list[str]:
    countries = yaml.safe_load(
        files("gaia.configs").joinpath("hdx_countries.yaml").read_text()
    )
    return sorted(countries.keys())


def check_hdx_pages(output_path: str) -> None:
    load_dotenv()
    hdx_config = _hdx_config_from_env()
    Configuration.create(
        hdx_site=hdx_config.site,
        user_agent="GaiaHdxPageChecker",
        hdx_key=hdx_config.api_key,
    )

    country_codes = load_country_codes()
    existing = []
    missing = []

    for country_code in country_codes:
        try:
            country_name = get_hdx_country(country_code)
        except ValueError as e:
            print(f"[{country_code}] Skipping, not resolvable to a country name: {e}")
            continue

        dataset_hdx_name = get_dataset_hdx_name(country_name)
        dataset = Dataset.read_from_hdx(dataset_hdx_name)

        if dataset:
            print(f"[{country_code}] HDX page exists: {dataset_hdx_name}")
            existing.append(country_code)
        else:
            print(f"[{country_code}] No HDX page: {dataset_hdx_name}")
            missing.append(country_code)

    print(
        f"\n{len(existing)}/{len(country_codes)} countries have a live HDX page."
    )

    with open(output_path, "w") as f:
        yaml.safe_dump(
            {
                "countries_with_hdx_page": existing,
                "countries_without_hdx_page": missing,
            },
            f,
            sort_keys=False,
        )

    print(f"Wrote results to {output_path}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Check which countries currently have a live HDX dataset page."
    )
    parser.add_argument(
        "--output",
        default="existing_hdx_pages.yaml",
        help="Path to write the YAML results to (default: existing_hdx_pages.yaml)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        check_hdx_pages(args.output)
    except Exception as e:
        print(f"Check failed: {e}")
        sys.exit(1)
