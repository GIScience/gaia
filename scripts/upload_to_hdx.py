import os
import yaml
import argparse
import sys
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset, HDXError
from datetime import datetime, timezone, timedelta

class Context:
    def info(self, msg):
        print(f"INFO: {msg}")

    def warning(self, msg):
        print(f"WARNING: {msg}")


def generate_links(country_code: str, local_folder: str):
    """Return list of (filename, public_download_url) for all files in the folder."""
    links = []
    for fname in os.listdir(local_folder):
        if fname.endswith(".DS_Store"):
            continue
        url = (
            f"https://warm.storage.heigit.org/heigit-hdx-public/risk_assessment_inputs/"
            f"{country_code.lower()}/{fname}"
        )
        links.append((fname, url))
    return links


def cyclone_files(links):
    """Return True if any filename contains 'cyclone'."""
    return any("cyclone" in fname.lower() for fname, _ in links)


def get_hdx_country(country_code: str, countries_config_path: str) -> str:
    """Get display name for a country from YAML mapping."""
    with open(countries_config_path, "r") as f:
        countries = yaml.safe_load(f)
    try:
        hdx_country = countries[country_code]["hdx_country"]
        return hdx_country.replace("-", " ").title()
    except KeyError:
        raise ValueError(
            f"Country code '{country_code}' not found in {countries_config_path}"
        )


def create_country_dataset(country_code: str, country_name: str, links, config, context: Context):
    """Create or update a 'Risk Assessment Indicators' dataset in HDX for the country."""
    dataset_name = f"{country_name} - Risk Assessment Indicators"
    dataset_hdx_country = dataset_name.lower().replace(" ", "-")

    include_cyclone = cyclone_files(links)

    cyclone_section = (
        f"""
#### **Cyclone Exposure (`{country_code}_ADM2_cyclone_exposure`)**
Represents the exposure of populations and facilities to cyclones, based on historical cyclone tracks and intensity categories (1–3). Vulnerable populations and facilities are quantified per admin unit.

- **ADM2_PCODE** – Administrative division code (ADM2)
- **kt34_female_pop_cat1 / cat2 / cat3**, **kt34_children_u5_cat1 / cat2 / cat3**, etc. – Population exposed to cyclone categories 1–3
- **kt34_education_perc / count_cat1 / cat2 / cat3**, **kt34_hospitals_perc / count_cat1 / cat2 / cat3**, **kt34_primary_healthcare_perc / count_cat1 / cat2 / cat3** – Facilities exposed to cyclone categories

Data Source: [IBTrACS – NOAA International Best Track Archive for Climate Stewardship](https://www.ncei.noaa.gov/products/international-best-track-archive-for-climate-stewardship-ibtracs)

<p>&nbsp;</p>
<p>&nbsp;</p>

---
"""
        if include_cyclone
        else ""
    )

    dataset_notes = f"""
This dataset provides comprehensive **Risk Assessment Indicators** for **{country_name}**, aggregated at **admin level 2**.
It includes demographic, environmental, infrastructure, accessibility, and hazard-related data to support disaster risk and resilience analysis.

All layers are derived from **HeiGIT’s GAIA Pipeline**, integrating open data sources such as [WorldPop](https://www.worldpop.org/), 
[OpenStreetMap](https://www.openstreetmap.org/), and [Google Earth Engine](https://earthengine.google.com/) based on 
[HDX COD-AB](https://data.humdata.org/dataset/?q=cod-ab) boundaries.

---

### **Data Overview**

- **Access to Services (`{country_code}_ADM2_access`)**  
- **Facilities (`{country_code}_ADM2_facilities`)**  
- **Coping Capacity (`{country_code}_ADM2_coping`)**  
- **Demographics (`{country_code}_ADM2_demographics`)**  
- **Rural Population (`{country_code}_ADM2_rural_population`)**  
- **Vulnerability (`{country_code}_ADM2_vulnerability`)**  
- **Flood Exposure (`{country_code}_ADM2_flood_exposure`)**  
{("- **Cyclone Exposure (`" + country_code + "_ADM2_cyclone_exposure`)**") if include_cyclone else ""}

<p>&nbsp;</p>
<p>&nbsp;</p>

---

### **Indicator Descriptions**

#### **Access to Services (`{country_code}_ADM2_access`)**
Represents the share of the population with access to key facilities within defined distances or travel times.

- **ADM2_PCODE** – Administrative division code (ADM2)
- **access_pop_education_5km / 10km / 20km** – Population within 5, 10, and 20 km of educational facilities
- **access_pop_hospitals_30min / 1h / 2h** – Population within 30 minutes, 1 hour, and 2 hours of a hospital
- **access_pop_primary_healthcare_30min / 1h / 2h** – Population within 30 minutes, 1 hour, and 2 hours of a primary health care facility

Data Source: [openrouteservice (ORS)](https://openrouteservice.org/)

---

#### **Facilities (`{country_code}_ADM2_facilities`)**
Counts of essential service facilities within each district.

- **ADM2_PCODE** – Administrative division code (ADM2)
- **education_count** – Number of educational facilities
- **hospitals_count** – Number of hospitals
- **primary_healthcare_count** – Number of primary health care facilities

Data Source: [OpenStreetMap (OSM)](https://www.openstreetmap.org)

---

#### **Coping Capacity (`{country_code}_ADM2_coping`)**
Combines **Access to Services** and **Facilities** data to represent a district’s coping capacity.

---

#### **Demographics (`{country_code}_ADM2_demographics`)**
Shows the population composition by age and gender.

- **ADM2_PCODE** – Administrative division code (ADM2)
- **female_pop**
- **children_u5**
- **female_u5**
- **elderly**
- **pop_u15**
- **female_u15**

Data Source: [Worldpop](https://www.worldpop.org/)

---

#### **Rural Population (`{country_code}_ADM2_rural_population`)**
Same demographic breakdown as above, but limited to rural populations.

Data Source: [Global Human Settlement Layer (GHSL)](https://human-settlement.emergency.copernicus.eu/datasets.php)

---

#### **Vulnerability (`{country_code}_ADM2_vulnerability`)**
Combines **Demographics** and **Rural Population** indicators.

---

#### **Flood Exposure (`{country_code}_ADM2_flood_exposure`)**
Shows population and facility exposure to flooding at 30 cm depth for multiple return periods.

Data Source: [JRC](https://data.jrc.ec.europa.eu/collection/id-0054)

---

{cyclone_section}

### **QGIS Plugin Risk Assessment Inputs**

- **Coping Capacity** = Access + Facilities  
- **Vulnerability** = Demographics + Rural Population  
- **Exposure** = Vulnerable Population + Facilities exposed to Floods{" and Cyclones" if include_cyclone else ""}

This dataset is part of HeiGIT’s **Risk Assessment Indicator Collection** on HDX.  
"""

    dataset = Dataset()
    dataset["name"] = dataset_hdx_country
    dataset["title"] = dataset_name
    dataset["dataset_type"] = "dataset_series"

    dataset["owner_org"] = config["hdx"]["owner_org"]
    dataset["private"] = config["hdx"].get("private", False)
    dataset.set_expected_update_frequency(
        config["hdx"].get("data_update_frequency", "Every six months")
    )
    dataset["license_id"] = "cc-by-sa"
    dataset["dataset_source"] = "Multiple sources"
    dataset["maintainer"] = config["hdx"].get("maintainer", "Valentin Boehmer")
    dataset["maintainer_email"] = config["hdx"].get(
        "maintainer_email", "valentin.boehmer@heigit.org"
    )

    dataset["methodology"] = (
        "This dataset aggregates multiple risk assessment indicators for the country."
    )

    dataset["data_series"] = (
        "Heidelberg Institute for Geoinformation Technology - Risk Assessment Indicators"
    )

    dataset["notes"] = dataset_notes

    dataset["tags"] = [
        {"name": "hazards and risk"},
        {"name": "health facilities"},
        {"name": "indicators"},
        {"name": "affected population"},
        {"name": "demographics"},
        {"name": "flooding"},
    ]

    # If cyclone files exist → add cyclone tag
    if include_cyclone:
        dataset["tags"].append({"name": "cyclones-hurricanes-typhoons"})

    try:
        dataset.add_country_location(country_code)
    except HDXError as e:
        print(f"Warning: {e}")

    # Set dataset time period: from six months ago to today
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=182)
    dataset["dataset_date"] = (
        f"[{start_date.strftime('%Y-%m-%dT%H:%M:%S')} TO "
        f"{end_date.strftime('%Y-%m-%dT%H:%M:%S')}]"
    )

    # Add or update resources (with logging)
    context.info("Uploading the following files to HDX:")
    for fname, url in links:
        context.info(f" - {fname} → {url}")
        resource = {
            "name": fname,
            "description": f"{fname} - Risk assessment indicator for {country_name}",
            "format": os.path.splitext(fname)[1][1:].upper(),
            "url": url,
        }
        dataset.add_update_resource(resource)

    dataset.create_in_hdx()
    return dataset.get_hdx_url()


def upload_to_hdx(
    country: str,
    config_file="configs/hdx_config.yaml",
    countries_config="configs/hdx_countries.yaml",
    context=Context()
):
    """Main entrypoint: upload all risk assessment files for a country to HDX."""

    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    country_name = get_hdx_country(country, countries_config)
    local_folder = os.path.join("data", country, "output")
    if not os.path.isdir(local_folder):
        raise FileNotFoundError(f"Folder not found: {local_folder}")

    links = generate_links(country, local_folder)

    Configuration.create(
        hdx_site=config["hdx"]["site"],
        user_agent="HDXDataSeriesScript",
        hdx_key=config["hdx"]["api_key"],
    )

    return create_country_dataset(country, country_name, links, config, context=context)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Upload all risk assessment indicator files to HDX."
    )
    parser.add_argument("country", help="ISO 3-letter country code (e.g. RWA)")
    parser.add_argument("config_file", help="Path to YAML config file")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        url = upload_to_hdx(args.country, args.config_file)
        print(f"Upload complete. Dataset available at: {url}")
    except Exception as e:
        print(f"Upload failed: {e}")
        sys.exit(1)