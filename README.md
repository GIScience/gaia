# GAIA: Global Aggregation of Indicators for Anticipatory Action

**GAIA** is a modular data pipeline built with [Dagster](https://dagster.io/) for aggregating global datasets to support **Anticipatory Action (AA)** initiatives. It enables the transformation of large-scale, open-source geospatial and statistical data into **actionable, admin-level indicators** for use in risk analysis, early warning, targeting, and forecasting.

---

## Overview

GAIA automates the collection, harmonization, and aggregation of global datasets on various administrative levels (ADM0, ADM1, ADM2). It is designed for:

- Humanitarian response
- Disaster risk reduction
- Development planning
- Data-driven AA project design

By standardizing indicators from diverse sources such as **WorldPop**, **HDX**, and **OpenStreetMap**, GAIA helps bridge the gap between global data and local action.

---

## Features

- **Geospatial aggregation** on customizable administrative boundaries  
- **Partitioned Dagster assets** for scalable pipeline orchestration  
- **Modular design**: run only the assets you need (e.g. population, facilities, accessibility)  
- **YAML-based configuration** for reproducibility and customization  
- **Integration-ready output** for dashboards, models, and AA planning tools

---

## Data Sources

GAIA is designed to integrate with publicly available global datasets, including:

- **[WorldPop](https://www.worldpop.org/)** – population estimates
- **[HDX](https://data.humdata.org/)** – boundaries
- **[OpenStreetMap](https://www.openstreetmap.org/)** – roads & facilities
- **[Joint Research Centre](https://data.jrc.ec.europa.eu/collection/id-0054)** – flood exposure
- Custom user-provided data (GeoJSON, TSV, CSV)

> You can plug in additional sources via YAML configs and extend the pipeline.

---

## Example Use Cases

- Aggregating **population by district** to estimate exposure to floods
- Combining **vulnerability indicators** for early action triggers
- Creating **custom datasets** for AA targeting, planning, or machine learning

---

## Architecture

GAIA is built with the **Dagster** data orchestrator and follows an asset-based design.

```txt
                +------------------+
                |  Boundary Loader |
                +------------------+
                        ↓
                +------------------+
                |  Data Ingestors  |
                | (WorldPop, HDX)  |
                +------------------+
                        ↓
                +------------------+
                | Data Processors  |
                | (spatial join,   |
                |  aggregation)    |
                +------------------+
                        ↓
                +------------------+
                | Final Output     |
                | (CSV)            |
                +------------------+
```
---

## Setup

**Python env**

Make sure you use python 3.12.10

```sh
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**MinIO config**

To enable the MinIO upload asset, create a YAML config (e.g. `minio_config.yaml`):

```yaml
minio_asset:
    endpoint: warm.storage.heigit.org
    bucket: heigit-hdx-public
    access_key: <ACESS_KEY>
    secret_key: <SECRET_KEY>
    dest_prefix: risk_assessment_inputs
    secure: true
```

Then, in your main assets config, point minio_asset.config_path at that file:

```yaml
minio_asset:
  config_path: path/to/minio_config.yaml

```

**HDX config**

To enable the HDX upload asset, create a YAML config (e.g. `hdx_config.yaml`) with:

```yaml
hdx:
  site: "prod"
  api_key: <YOUR API KEY>
  owner_org: <YOUR OWNER_ORG> #e.g"heidelberg-institute-for-geoinformation-technology"
  data_update_frequency: "Every six months"
  maintainer: <MAINTAINER NAME>
  maintainer_email: <MAINTAINER E-MAIL>
  private: true
```

Then, in your main assets config, point hdx_asset.config_path at that file:

```yaml
hdx_asset:
  config_path: path/to/hdx_config.yaml
```

## Configure and start up dagster interactively

In your main assets config, set up the desired year you want to use in your ndvi_asset:
```yaml
ndvi_asset:
  year: [year] #e.g. [2022]
```

In your main assets config, set up the desired admin levels in your main asset configuration:

```yaml
setup:
  admin_levels: ['ADM*','ADM**']
```

Then start the dagster interface at port 4444. Can be changed to any other via the `-p` flag.

```sh
export DAGSTER_HOME="$PWD/.dagster"
dagster dev  -w workspace.yml -p 4444
```
