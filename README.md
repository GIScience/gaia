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
- **Typed config + environment variables** for reproducible setup  
- **Integration-ready output** for dashboards, models, and AA planning tools

---

## Data Sources

GAIA is designed to integrate with publicly available global datasets, including:

- **[WorldPop](https://www.worldpop.org/)** – population estimates
- **[HDX](https://data.humdata.org/)** – boundaries
- **[OpenStreetMap](https://www.openstreetmap.org/)** – roads & facilities
- **[Joint Research Centre](https://data.jrc.ec.europa.eu/collection/id-0054)** – flood exposure
- Custom user-provided data (GeoJSON, TSV, CSV)

> You can plug in additional sources by adding assets and extend the pipeline.

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

**Prerequisites**

- Python 3.10–3.13 (3.12 recommended)
- [uv](https://docs.astral.sh/uv/) for dependency management

**Install**

```sh
uv sync
source .venv/bin/activate
```

`uv sync` creates `.venv` and installs every dependency pinned in `uv.lock` (including `dagster==1.12.4` from `pyproject.toml`).

**Configuration**

Configuration is handled by typed Dagster `Config` classes (see `src/gaia/defs/constants.py`) and environment variables — the old YAML asset configs are gone.

Sensible defaults are baked in:

| Setting | Default |
|---|---|
| admin levels | `["ADM2"]` |
| return periods (`rps`) | `["10", "50", "100", "500"]` |
| flood threshold | `0.3` m |
| facilities API | `ohsome-api` |
| crops years | `[2023, 2024]` |
| NDVI year | `[2022]` |
| ACLED year | `2021` |

Override them per run from the Dagster UI by passing run config when materializing a partition, e.g.:

```yaml
ops:
  demographics_asset:
    config:
      admin_levels: ["ADM1", "ADM2"]
```

**Environment variables**

Create a `.env` file in the project root — `dg dev` loads it automatically.

```sh
DAGSTER_HOME="$PWD/.dagster"

# Ohsome API (used by the facilities asset)
OHSOME_API_KEY=<YOUR_KEY>

# S3 upload (visualization / upload assets)
S3_ENDPOINT=hot.storage.heigit.org
S3_BUCKET=heigit-hdx-public
S3_ACCESS_KEY=<ACCESS_KEY>
S3_SECRET_KEY=<SECRET_KEY>
S3_DEST_PREFIX=risk_assessment_inputs
S3_SECURE=true

# HDX upload
HDX_API_KEY=<YOUR_API_KEY>
HDX_OWNER_ORG=heidelberg-institute-for-geoinformation-technology
HDX_DATA_UPDATE_FREQUENCY=Every six months
HDX_MAINTAINER=<MAINTAINER NAME>
HDX_MAINTAINER_EMAIL=<MAINTAINER E-MAIL>
HDX_PRIVATE=false
```

The S3 and HDX variables are only needed for the upload assets; the core `local_workflow_job` runs without them.

## Start Dagster

```sh
dg dev -p 4444
```

`dg dev` (from the `dagster-dg-cli` package) loads the project configured in `pyproject.toml` (`[tool.dg]`) together with `src/gaia/definitions.py`, and starts both the Dagster UI and the daemon. The daemon must be running for the sensor to tick. Open http://localhost:4444; change the port with `-p`.

## Running the pipeline

- All assets are **partitioned per country** — 146 ISO3 codes from `src/gaia/configs/hdx_countries.yaml`. Pick a country partition when materializing.
- `local_workflow_job` — runs the full indicator chain (boundaries, demographics, facilities, exposure, RAI, access, coping, vulnerability, …) for a single country.
- `country_workflow_sensor` — activate it under **Automation → Sensors**. Every tick it launches the next unprocessed country via `local_workflow_job` while keeping at most **3 runs in flight**, so all countries get processed exactly once with constant, low concurrency.
- `visualization_job` — combines the indicators into risk scores and PMTiles, then uploads to S3.
