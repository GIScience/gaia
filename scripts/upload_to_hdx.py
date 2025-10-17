import os
import yaml
import argparse
import sys
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from datetime import datetime, timezone


def generate_links(country_code: str, local_folder: str, dataset_type: str):
    """Return list of (filename, public_download_url) filtered by dataset_type."""
    links = []
    for fname in os.listdir(local_folder):
        if fname.endswith(".DS_Store"):
            continue
        if dataset_type not in fname.lower():
            continue

        url = f"https://warm.storage.heigit.org/heigit-hdx-public/{dataset_type}/{country_code.lower()}/{fname}"
        links.append((fname, url))
    return links


def get_country_slug(country_code: str, countries_config_path: str) -> str:
    """Get display name for a country from YAML mapping."""
    with open(countries_config_path, "r") as f:
        countries = yaml.safe_load(f)
    try:
        slug = countries[country_code]["slug"]
        return slug.replace("-", " ").title()
    except KeyError:
        raise ValueError(f"Country code '{country_code}' not found in {countries_config_path}")


def create_country_dataset(country_code: str, country_name: str, links, config, dataset_type: str):
    """Create or update a dataset in HDX with given metadata and resources."""
    dataset_name = f"{country_name} {dataset_type.title()}"
    title = f"{country_name} - {dataset_type.title()}"

    dataset = Dataset()
    dataset["name"] = dataset_name.lower().replace(" ", "-")
    dataset["title"] = title
    dataset["owner_org"] = config["hdx"]["owner_org"]
    dataset["groups"] = [{"name": config["hdx"]["owner_org"]}]
    dataset["private"] = config["hdx"].get("private", False)
    dataset.set_expected_update_frequency(config["hdx"].get("data_update_frequency", "Every six months"))
    dataset["license_id"] = "cc-by-sa"
    dataset["dataset_source"] = "HeiGIT"
    dataset["maintainer"] = config["hdx"].get("maintainer", "Valentin Boehmer")
    dataset["maintainer_email"] = config["hdx"].get("maintainer_email", "valentin.boehmer@heigit.org")

    if dataset_type == "demographics":
        dataset["methodology"] = "Population indicators aggregated from WorldPop and administrative boundary datasets."
        dataset["notes"] = (
            f"This dataset provides demographic population indicators for {country_name}, aggregated at different administrative levels. "
            "The indicators are derived from [WorldPop](https://www.worldpop.org/) population raster data at 100m resolution. "
            "We overlay these with official [COD-AB](https://data.humdata.org/) boundaries.\n\n"
            "Includes key population subgroups: female population, children under 5, elderly, etc.\n\n"
            "Data Structure:\n\n"
            "- **ADM_PCODE**: Unique unit identifier\n"
            "- **female_pop**: Total female population\n"
            "- **children_u5**: Number of children under 5 years old\n"
            "- **female_u5**: Female children under 5 years old\n"
            "- **elderly**: Population over 65 years old\n"
            "- **pop_u15**: Population under 15 years old\n"
            "- **female_u15**: Female population under 15 years old\n\n"
            "Source: [WorldPop](https://www.worldpop.org/) and [HDX COD-AB](https://data.humdata.org/)\n"
        )
        dataset["tags"] = [{"name": "population"}, {"name": "demographics"}]

    if dataset_type == "facilities":
        dataset["methodology"] = "Locations of education & health facilities at various administrative levels."
        dataset["notes"] = (
            f"This dataset provides the locations of health and education facilities for {country_name}, aggregated at different administrative levels. "
            "The data was provided by [OpenStreetMap](https://www.openstreetmap.org/) and aggregated with COD-AB boundaries.\n\n"
            "Data Structure:\n\n"
            "- **ADM_PCODE**: Unique unit identifier\n"
            "- **schools**: Number of education facilities\n"
            "- **hospitals**: Number of hospitals\n"
            "- **primary_healthcare**: Number of primary healthcare facilities\n\n"
            "Source: [OpenStreetMap](https://www.openstreetmap.org/)\n"
        )
        dataset["tags"] = [{"name": "health facilities"}, {"name": "education"}]

    try:
        dataset.add_country_location(country_code)
    except HDXError as e:
        print(f"Warning: {e}")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    dataset["dataset_date"] = f"[{today} TO {today}]"

    for fname, url in links:
        resource = {
            "name": fname,
            "description": f"{fname} {dataset_type} data for {country_name}",
            "format": os.path.splitext(fname)[1][1:].upper(),
            "url": url,
        }
        dataset.add_update_resource(resource)

    dataset.create_in_hdx()
    return dataset.get_hdx_url()


def upload_to_hdx(country: str, dataset_type: str, config_file="configs/hdx_config.yaml", countries_config="configs/hdx_countries.yaml"):
    """Main entrypoint: upload dataset to HDX for given country and type."""
    dataset_type = dataset_type.lower()

    # Load config
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    country_name = get_country_slug(country, countries_config)
    local_folder = os.path.join("data", country, "output")
    if not os.path.isdir(local_folder):
        raise FileNotFoundError(f"Folder not found: {local_folder}")

    links = generate_links(country, local_folder, dataset_type)

    # Setup HDX API
    Configuration.create(
        hdx_site=config["hdx"]["site"],
        user_agent="HDXUploadScript",
        hdx_key=config["hdx"]["api_key"],
    )

    return create_country_dataset(country, country_name, links, config, dataset_type)


def parse_args():
    parser = argparse.ArgumentParser(description="Upload datasets (demographics/facilities) to HDX.")
    parser.add_argument("country", help="ISO 3-letter country code (e.g. RWA)")
    parser.add_argument("config_file", help="Path to YAML config file")
    parser.add_argument("dataset_type", choices=["demographics", "facilities"])
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    try:
        url = upload_to_hdx(args.country, args.dataset_type, args.config_file)
        print(f"Upload complete. Dataset available at: {url}")
    except Exception as e:
        print(f"Upload failed: {e}")
        sys.exit(1)