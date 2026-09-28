# Multi-Source API Data Integration
A configuration-driven framework that brings data from multiple APIs into one consistent pipeline. Raw responses are saved in Bronze, then cleaned and loaded into Silver. Adding a new API takes only a config entry, not a new pipeline.

## What It Does

- Fetches data from multiple APIs
- Handles API authentication, pagination, and retries
- Stores raw API responses in Bronze tables
- Transforms and cleans data into Silver tables
- Supports full and incremental ingestion

## APIs

| API | Data | Load Type |
|---|---|---|
| GitHub | Apache Spark commits | Incremental |
| OpenWeather | 5-day weather forecast (Chennai) | Full |
| ExchangeRate | USD exchange rates | Full |

## Project Structure

```text
multi-source-api-integration/
│
├── config/
│   └── api_config.json
│
├── src/
│   ├── api_client.py
│   ├── ingestion.py
│   ├── transformation.py
│   └── state.py
│
├── notebooks/
│   ├── 00_Setup.py
│   ├── 01_API_Ingestion.py
│   └── 02_Bronze_To_Silver.py
│
└── README.md
```

## Pipeline

```text
APIs
    ↓
API Configuration
    ↓
01_API_Ingestion
    ↓
Bronze
    ↓
02_Bronze_To_Silver
    ↓
Silver
```

## Notebooks

**00_Setup.py** — Creates the Bronze, Silver, and config schemas and the tracking table that stores each API's progress.

**01_API_Ingestion.py** — Calls the configured APIs, handles authentication, pagination, and retries, and writes the raw data to Bronze.

**02_Bronze_To_Silver.py** — Parses and flattens the JSON, standardizes column names, removes duplicates, and loads the data into Silver.

## Configuration

API details are maintained in:

```text
config/api_config.json
```

To add another API, add a new configuration block instead of creating a new pipeline.

## Setup

1. Clone the project into Databricks Repos.
2. Edit `config/api_config.json` — set `catalog` and the schema names.
3. Create the secret scope and store the keys for the sources you enable. GitHub needs no key.

   ```bash
   databricks secrets create-scope api_integration
   databricks secrets put-secret api_integration openweather_api_key
   databricks secrets put-secret api_integration exchangerate_api_key
   ```

4. Run `notebooks/00_Setup`.
5. Run `notebooks/01_API_Ingestion`, then `notebooks/02_Bronze_To_Silver`.
6. Schedule 01 and 02 as a two-task Databricks Workflow.

## Prerequisites

- Databricks workspace with Unity Catalog and Databricks Repos
- Serverless or compatible Databricks compute
- Permission to create schemas and Delta tables
- Databricks Secret Scope
- API keys for OpenWeather and ExchangeRate

## Sources

| Library | License | Source |
|---|---|---|
| Apache Spark / PySpark | Apache License 2.0 | [Apache Spark](https://github.com/apache/spark) |
| Delta Lake | Apache License 2.0 | [Delta Lake](https://github.com/delta-io/delta) |
| Requests | Apache License 2.0 | [Requests](https://github.com/psf/requests) |
