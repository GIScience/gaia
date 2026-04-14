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
            f"https://hot.storage.heigit.org/heigit-hdx-public/risk_assessment_inputs/"
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
    

def smart_upload_to_hdx(country_code, file_map, config_file, countries_config, context):
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    country_name = get_hdx_country(country_code, countries_config)
    
    Configuration.create(
        hdx_site=config["hdx"]["site"],
        user_agent="GaiaSmartUploader",
        hdx_key=config["hdx"]["api_key"],
    )

    links = []
    for label, local_path in file_map.items():
        # local_path is now a string from our os.path.join above
        fname = os.path.basename(local_path)
        url = f"https://hot.storage.heigit.org/heigit-hdx-public/risk_assessment_inputs/{country_code.lower()}/{fname}"
        links.append((fname, url))

    return create_country_dataset(country_code, country_name, links, config, context)


def create_country_dataset(country_code: str, country_name: str, links, config, context):
    """
    Smart Create or Update:
    - If dataset exists, updates metadata and specific resources.
    - If links is empty, only updates metadata/notes.
    - Preserves all your original indicator descriptions.
    """
    # 1. Setup Naming
    dataset_name = f"{country_name} - Risk Assessment Indicators"
    dataset_hdx_name = dataset_name.lower().replace(" ", "-").replace("(", "").replace(")", "")

    # 2. Smart Dataset Retrieval / Creation
    dataset = Dataset.read_from_hdx(dataset_hdx_name)
    
    existing_cyclone = False
    if dataset:
        context.log.info(f"Dataset '{dataset_hdx_name}' already exists. Updating metadata.")
        for res in dataset.get_resources():
            if res.get("name") and "cyclone" in res["name"].lower():
                existing_cyclone = True
    else:
        context.log.info(f"Dataset '{dataset_hdx_name}' not found. Creating a new one.")
        dataset = Dataset({
            "name": dataset_hdx_name,
            "title": dataset_name
        })

    # 3. Check for Cyclone exposure to toggle sections
    include_cyclone = cyclone_files(links) or existing_cyclone
    
    # --- Start of your original Dataset Notes ---
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
This dataset provides comprehensive **Risk Assessment Indicators** for **{country_name}**, aggregated at **admin level 2** and 
can in particular be used to perform a structured risk assessment for **flood** {("and **cyclone** hazards." if include_cyclone else "hazards.")}
It includes demographic, environmental, infrastructure, accessibility, and hazard-related data to support disaster risk and resilience analysis.

All layers are derived from [HeiGIT’s GAIA Pipeline](https://giscience.github.io/gis-training-resource-center/content/GIS_AA/en_gaia_indicators_processing.html), integrating open data sources such as [WorldPop](https://www.worldpop.org/), 
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
- **female_pop** – Total female population
- **children_u5** – Population under 5 years old
- **female_u5** – Female population under 5 years old
- **elderly** – Population aged 65 and older
- **pop_u15** – Population under 15 years old
- **female_u15** – Female population under 15 years old

Data Source: [Worldpop](https://www.worldpop.org/)

---

#### **Rural Population (`{country_code}_ADM2_rural_population`)**
Same demographic breakdown as above, but limited to rural populations. Rural areas are those outside urban extents,
typically characterized by lower population density, agricultural or natural land use, and limited infrastructure compared to urban centers.

- **ADM2_PCODE** – Administrative division code (ADM2)
- **female_pop_rural**, **children_u5_rural**, **female_u5_rural**, **elderly_rural**, **pop_u15_rural**, **female_u15_rural** – Rural demographic counts
- **rural_pop_perc** – Percentage of total population living in rural areas

Data Source: [Global Human Settlement Layer (GHSL)](https://human-settlement.emergency.copernicus.eu/datasets.php)

---

#### **Vulnerability (`{country_code}_ADM2_vulnerability`)**
Combines **Demographics** and **Rural Population** indicators.

---

#### **Flood Exposure (`{country_code}_ADM2_flood_exposure`)**
Shows population and facility exposure to flooding at 30 cm depth for multiple return periods.

- **ADM2_PCODE** – Administrative division code (ADM2)
- **female_pop_30cm**, **children_u5_30cm**, **female_u5_30cm**, **elderly_30cm**, **pop_u15_30cm**, **female_u15_30cm** – Exposed population by group
- **education_30cm_pct / count**, **hospitals_30cm_pct / count**, **primary_healthcare_30cm_pct / count** – Facility exposure (percentage and count)

Data Source: [The Joint Research Centre (JRC)](https://data.jrc.ec.europa.eu/collection/id-0054)


---

{cyclone_section}

### **QGIS Plugin Risk Assessment Inputs**

- **Coping Capacity** = Access + Facilities  
- **Vulnerability** = Demographics + Rural Population  
- **Exposure** = Vulnerable Population + Facilities exposed to Floods{" and Cyclones" if include_cyclone else ""}

This dataset is part of HeiGIT’s **Risk Assessment Indicator Collection** on HDX.
See more at [HeiGIT on HDX](https://data.humdata.org/organization/heidelberg-institute-for-geoinformation-technology) and learn about HeiGIT’s research at [HeiGIT](https://heigit.org/).  

We are happy to hear about your use-cases — contact us at [communications@heigit.org](mailto:communications@heigit.org)!
"""
    # --- End of original Dataset Notes ---

    # 4. Set/Update Static Metadata
    dataset["dataset_type"] = "dataset_series"
    dataset["owner_org"] = config["hdx"]["owner_org"]
    dataset["private"] = config["hdx"].get("private", False)
    dataset.set_expected_update_frequency(config["hdx"].get("data_update_frequency", "Every six months"))
    dataset["license_id"] = "cc-by-sa"
    dataset["dataset_source"] = "Multiple sources"
    dataset["maintainer"] = config["hdx"].get("maintainer", "Valentin Boehmer")
    dataset["maintainer_email"] = config["hdx"].get("maintainer_email", "valentin.boehmer@heigit.org")
    dataset["methodology"] = "Other"
    dataset["methodology_other"] = "This dataset aggregates multiple risk assessment indicators for the country."
    dataset["data_series"] = "Heidelberg Institute for Geoinformation Technology - Risk Assessment Indicators"
    dataset["subnational"] = "1"
    dataset["notes"] = dataset_notes

    # 5. Handle Tags
    tags = ["hazards and risk", "health facilities", "indicators", "affected population", "demographics", "flooding"]
    if include_cyclone:
        tags.append("cyclones-hurricanes-typhoons")
    dataset.add_tags(tags)

    # 6. Set Time Period & Location
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=182)
    
    # Use direct dictionary assignment like your original working script:
    dataset["dataset_date"] = (
        f"[{start_date.strftime('%Y-%m-%dT%H:%M:%S')} TO "
        f"{end_date.strftime('%Y-%m-%dT%H:%M:%S')}]"
    )

    # Ensure country_code is a clean ISO3 string
    iso3_code = country_code.strip().upper()
    
    try:
        # This is the "groups" field HDX is looking for
        dataset.add_country_location(iso3_code)
        context.log.info(f"Added location: {iso3_code}")
    except Exception as e:
        context.log.warning(f"Could not add location {iso3_code}: {e}")
        # Manual fallback if add_country_location fails
        dataset['groups'] = [{'name': iso3_code.lower()}]

    # 7. Smart Resource Management
    if links:
        context.log.info(f"Uploading/Updating {len(links)} resources to HDX.")
        for fname, url in links:
            resource = {
                "name": fname,
                "description": f"{fname} - Risk assessment indicator for {country_name}",
                "format": os.path.splitext(fname)[1][1:].upper(),
                "url": url,
            }
            # add_update_resource will overwrite an existing resource with the same name
            dataset.add_update_resource(resource)
    else:
        context.log.warning("No new resource files provided by assets. Metadata updated, existing files preserved.")

    # 8. Commit to HDX
    if dataset.get("id"):
        dataset.update_in_hdx()
    else:
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
    local_folder = os.path.join("data", country, "Output")
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