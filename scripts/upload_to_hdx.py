import os
import yaml
import argparse
import sys
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from datetime import datetime, timezone



def generate_links(country_code: str, local_folder: str):
    """Return list of (filename, public_download_url) for all files in the folder."""
    links = []
    for fname in os.listdir(local_folder):
        if fname.endswith(".DS_Store"):
            continue
        url = f"https://warm.storage.heigit.org/heigit-hdx-public/risk_assessment_inputs/{country_code.lower()}/{fname}"
        links.append((fname, url))
    return links


def get_hdx_country(country_code: str, countries_config_path: str) -> str:
    """Get display name for a country from YAML mapping."""
    with open(countries_config_path, "r") as f:
        countries = yaml.safe_load(f)
    try:
        hdx_country = countries[country_code]["hdx_country"]
        return hdx_country.replace("-", " ").title()
    except KeyError:
        raise ValueError(f"Country code '{country_code}' not found in {countries_config_path}")


def create_country_dataset(country_code: str, country_name: str, links, config):
    """Create or update a 'Risk Assessment Indicators' dataset in HDX for the country."""
    dataset_name = f"{country_name} - Risk Assessment Indicators"
    dataset_hdx_country = dataset_name.lower().replace(" ", "-")

    dataset = Dataset()
    dataset["name"] = dataset_hdx_country
    dataset["title"] = dataset_name
    dataset["owner_org"] = config["hdx"]["owner_org"]
    dataset["groups"] = [{"name": config["hdx"]["owner_org"]}]
    dataset["private"] = config["hdx"].get("private", False)
    dataset.set_expected_update_frequency(config["hdx"].get("data_update_frequency", "Every six months"))
    dataset["license_id"] = "cc-by-sa"
    dataset["dataset_source"] = "HeiGIT"
    dataset["maintainer"] = config["hdx"].get("maintainer", "Valentin Boehmer")
    dataset["maintainer_email"] = config["hdx"].get("maintainer_email", "valentin.boehmer@heigit.org")

    dataset["methodology"] = (
        "This dataset aggregates multiple risk assessment indicators for the country, "
        "including demographics, facilities, environmental, and hazard data."
        "Data sources include WorldPop, OpenStreetMap, HDX COD-AB, and other publicly available datasets. "
        "All indicators were processed and harmonized by HeiGIT's GAIA Pipeline."
    )

    dataset["notes"] = f"""
This dataset provides comprehensive **Risk Assessment Indicators** for **{country_name}**, aggregated at **admin level 2**.
It includes demographic, environmental, infrastructure, accessibility, and hazard-related data to support disaster risk and resilience analysis.

All layers are derived from **HeiGIT’s GAIA Pipeline**, integrating open data sources such as [WorldPop](https://www.worldpop.org/), [OpenStreetMap](https://www.openstreetmap.org/), and [Google Earth Engine](https://earthengine.google.com/) based on [HDX COD-AB](https://data.humdata.org/dataset/?q=cod-ab) boundaries.
For further information on the workflow and the processing steps of each layer, please visit the [GAIA Pipeline Documentation](https://giscience.github.io/gis-training-resource-center/content/GIS_AA/en_gaia_indicators_processing.html) and the [GAIA repository on GitHub](https://github.com/GIScience/gaia).

---

### **Data Overview**

The dataset contains multiple thematic layers:

- **Access to Services (`RWA_ADM2_access`)** – Population accessibility to education and health facilities.
- **Facilities (`RWA_ADM2_facilities`)** – Availability of key service infrastructure.
- **Coping Capacity (`RWA_ADM2_coping`)** – Combined indicators from Access and Facilities.
- **Demographics (`RWA_ADM2_demographics`)** – Age and gender distribution.
- **Rural Population (`RWA_ADM2_rural_population`)** – Demographic indicators limited to rural populations.
- **Vulnerability (`RWA_ADM2_vulnerability`)** – Combined indicators from Demographics and Rural Population.
- **Crop Coverage and Change (`RWA_ADM2_crops`)** – Agricultural land extent and changes.
- **Vegetation Index (`RWA_ADM2_ndvi`)** – NDVI-based vegetation conditions.
- **Flood Exposure (`RWA_ADM2_flood_exposure`)** – Exposure of populations and facilities to flood hazards.


<p>&nbsp;</p>
<p>&nbsp;</p>

---

### **Indicator Descriptions**

#### **Access to Services (`RWA_ADM2_access`)**
Represents the share of the population with access to key facilities within defined distances or travel times.

- **ADM2_PCODE** – Administrative division code (ADM2)
- **access_pop_education_5km / 10km / 20km** – Population within 5, 10, and 20 km of educational facilities
- **access_pop_hospitals_30min / 1h / 2h** – Population within 30 minutes, 1 hour, and 2 hours of a hospital
- **access_pop_primary_healthcare_30min / 1h / 2h** – Population within 30 minutes, 1 hour, and 2 hours of a primary health care facility

Data Source: [openrouteservice (ORS)](https://openrouteservice.org/)

---

#### **Facilities (`RWA_ADM2_facilities`)**
Counts of essential service facilities within each district.

- **ADM2_PCODE** – Administrative division code (ADM2)
- **education_count** – Number of educational facilities
- **hospitals_count** – Number of hospitals
- **primary_healthcare_count** – Number of primary health care facilities

Data Source: [OpenStreetMap (OSM)](https://www.openstreetmap.org)

---

#### **Coping Capacity (`RWA_ADM2_coping`)**
Combines **Access to Services** and **Facilities** data to represent a district’s coping capacity.

---

#### **Demographics (`RWA_ADM2_demographics`)**
Shows the population composition by age and gender.

- **ADM2_PCODE** – Administrative division code (ADM2)
- **female_pop** – Total female population
- **children_u5** – Population under 5 years old
- **female_u5** – Female population under 5 years old
- **elderly** – Population aged 65 and older
- **pop_u15** – Population under 15 years old
- **female_u15** – Female population under 15 years old

Data Source: [Worldpop](https://www.worldpop.org/)

---

#### **Rural Population (`RWA_ADM2_rural_population`)**
Same demographic breakdown as above, but limited to rural populations. Rural areas are those outside urban extents,
typically characterized by lower population density, agricultural or natural land use, and limited infrastructure compared to urban centers.

- **ADM2_PCODE** – Administrative division code (ADM2)
- **female_pop_rural**, **children_u5_rural**, **female_u5_rural**, **elderly_rural**, **pop_u15_rural**, **female_u15_rural** – Rural demographic counts
- **rural_pop_perc** – Percentage of total population living in rural areas

Data Source: [Global Human Settlement Layer (GHSL)](https://human-settlement.emergency.copernicus.eu/datasets.php)

---

#### **Vulnerability (`RWA_ADM2_vulnerability`)**
Combines **Demographics** and **Rural Population** indicators to represent socio-demographic vulnerability.

---

#### **Flood Exposure (`RWA_ADM2_flood_exposure`)**
Shows population and facility exposure to flooding at 30 cm depth for multiple return periods (10-, 50-, 100-, and 500-year). Each prefix (RP10, RP50, RP100, RP500) indicates the return period scenario.  

For each scenario, indicators include:

- **female_pop_30cm**, **children_u5_30cm**, **female_u5_30cm**, **elderly_30cm**, **pop_u15_30cm**, **female_u15_30cm** – Exposed population by group
- **education_30cm_pct / count**, **hospitals_30cm_pct / count**, **primary_healthcare_30cm_pct / count** – Facility exposure (percentage and count)

Data Source: [The Joint Research Centre (JRC)](https://data.jrc.ec.europa.eu/collection/id-0054)

<p>&nbsp;</p>
<p>&nbsp;</p>

---

### **QGIS Plugin Risk Assessment Inputs**

- **Coping Capacity** = Access + Facilities  
- **Vulnerability** = Demographics + Rural Population  
- **Exposure** = Vulnerable Population + Facilities exposed to Floods
---

This dataset is part of HeiGIT’s **Risk Assessment Indicator Collection** on HDX.  
See more at [HeiGIT on HDX](https://data.humdata.org/organization/heidelberg-institute-for-geoinformation-technology) and learn about HeiGIT’s research at [HeiGIT](https://heigit.org/).  

We are happy to hear about your use-cases — contact us at [communications@heigit.org](mailto:communications@heigit.org)!
"""

    dataset["tags"] = [
        {"name": "hazards and risk"},
        {"name": "health facilities"},
        {"name": "indicators"},
        {"name": "affected population"},
        {"name": "demographics"},
        {"name": "flooding"},
    ]

    try:
        dataset.add_country_location(country_code)
    except HDXError as e:
        print(f"Warning: {e}")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    dataset["dataset_date"] = f"[{today} TO {today}]"

    for fname, url in links:
        resource = {
            "name": fname,
            "description": f"{fname} - Risk assessment indicator for {country_name}",
            "format": os.path.splitext(fname)[1][1:].upper(),
            "url": url,
        }
        dataset.add_update_resource(resource)

    dataset.create_in_hdx()
    return dataset.get_hdx_url()


def upload_to_hdx(country: str, config_file="configs/hdx_config.yaml", countries_config="configs/hdx_countries.yaml"):
    """Main entrypoint: upload all risk assessment files for a country to HDX."""
    # Load config
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    country_name = get_hdx_country(country, countries_config)
    local_folder = os.path.join("data", country, "output")
    if not os.path.isdir(local_folder):
        raise FileNotFoundError(f"Folder not found: {local_folder}")

    links = generate_links(country, local_folder)

    # Setup HDX API
    Configuration.create(
        hdx_site=config["hdx"]["site"],
        user_agent="HDXUploadScript",
        hdx_key=config["hdx"]["api_key"],
    )

    return create_country_dataset(country, country_name, links, config)


def parse_args():
    parser = argparse.ArgumentParser(description="Upload all risk assessment indicator files to HDX.")
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