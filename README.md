# Weather Prediction Analytics

## Project Overview

This project implements an end-to-end weather analytics pipeline using Apache Airflow, Snowflake, dbt, and Preset / Apache Superset.

The pipeline collects weather data for San Jose and San Francisco from the Open-Meteo API, loads the raw data into Snowflake, transforms the data using dbt, calculates analytical weather metrics, validates the transformed data, maintains historical snapshots, and prepares the final dataset for dashboard visualization.

---

## Problem Statement

Weather data by itself is difficult to interpret when looking only at daily raw values.

The goal of this project is to build an automated data pipeline that:

- collects weather data from multiple cities
- stores the data in a cloud data warehouse
- transforms raw weather data into meaningful analytical metrics
- validates the transformed data
- maintains historical snapshots
- visualizes the results using a BI dashboard

---

## Project Architecture

```text
Open-Meteo API
      |
      v
Apache Airflow
      |
      v
Snowflake
DEV.RAW.WEATHER_DATA
      |
      v
dbt Transform Model
stg_weather_data
      |
      v
dbt Analytics Model
weather_metrics
      |
      v
Snowflake
DEV.ANALYTICS.WEATHER_METRICS
      |
      v
Preset / Apache Superset
Dashboard
```

---

## Technologies Used

| Technology | Purpose |
|---|---|
| Python | API extraction and ETL logic |
| Apache Airflow | Workflow orchestration and scheduling |
| Docker | Running Airflow and supporting services |
| Snowflake | Cloud data warehouse |
| dbt | Data transformation, testing, and snapshots |
| Open-Meteo API | Weather data source |
| Preset / Apache Superset | BI dashboard and visualization |
| SQL | Data loading, transformation, and validation |
| Git | Version control |
| GitHub | Code repository and team collaboration |

---

## Key Features

- Weather data extraction for San Jose and San Francisco
- Automated Airflow ETL pipeline
- Snowflake raw data storage
- Transaction-based idempotent loading
- dbt staging and analytics models
- 7-day moving average temperature
- 7-day rolling rainfall
- temperature anomaly calculation
- dbt data quality tests
- duplicate city/date validation
- dbt snapshots for historical tracking
- Airflow and dbt integration
- BI visualization using Preset / Apache Superset

---

## Data Source

The project uses the Open-Meteo Forecast API:

`https://api.open-meteo.com/v1/forecast`

The pipeline retrieves weather data for:

- San Jose, California
- San Francisco, California

The API request uses:

- `past_days = 60`
- `forecast_days = 1`

Daily fields retrieved include:

- `temperature_2m_max`
- `temperature_2m_min`
- `precipitation_sum`
- `weather_code`

---

## Airflow Pipeline

The main Airflow DAG is:

`weather_etl`

The pipeline contains the following tasks:

```text
extract_weather
       \
        -> load_weather -> dbt_run -> dbt_test -> dbt_snapshot
       /
extract_weather_1
```

The pipeline performs the following steps:

- `extract_weather` retrieves weather data for San Jose from the Open-Meteo API.
- `extract_weather_1` retrieves weather data for San Francisco from the Open-Meteo API.
- `load_weather` loads the extracted records into `DEV.RAW.WEATHER_DATA` in Snowflake.
- `dbt_run` executes the dbt transformation models.
- `dbt_test` runs the dbt data quality tests.
- `dbt_snapshot` creates or updates the dbt snapshot for historical tracking.

The dbt tasks only run after the Snowflake load task completes successfully.

---

## Airflow Variables

The following Airflow Variables are required:

- `weather_api_url`
- `san_jose_latitude`
- `san_jose_longitude`
- `san_francisco_latitude`
- `san_francisco_longitude`

Example values:

```text
weather_api_url = https://api.open-meteo.com/v1/forecast

san_jose_latitude = 37.3382
san_jose_longitude = -121.8863

san_francisco_latitude = 37.7749
san_francisco_longitude = -122.4194
```

---

## Snowflake Configuration

The project uses:

```text
Database: DEV
Raw Schema: RAW
Analytics Schema: ANALYTICS
Warehouse: COMPUTE_WH
```

---

## Raw Weather Table

The raw weather data is stored in:

`DEV.RAW.WEATHER_DATA`

### Table Structure

| Column | Description |
|---|---|
| `CITY` | City name |
| `LATITUDE` | City latitude |
| `LONGITUDE` | City longitude |
| `DATE` | Weather observation date |
| `TEMP_MAX` | Maximum daily temperature |
| `TEMP_MIN` | Minimum daily temperature |
| `PRECIPITATION` | Daily precipitation |
| `WEATHER_CODE` | Open-Meteo weather code |

The combination of `CITY` and `DATE` is used as the primary key.

---

## Idempotency and Transactions

The Airflow load task uses a Snowflake SQL transaction.

The loading process follows:

```text
BEGIN
DELETE
INSERT
COMMIT
```

If an error occurs:

```text
ROLLBACK
```

is executed and the exception is raised.

This allows the pipeline to be rerun without creating duplicate data.

---

## dbt Project

The dbt project is located at:

`dbt/weather_analytics`

The project contains:

- models
- tests
- snapshots
- source configuration
- dbt project configuration

---

## dbt Source

The Snowflake raw table is configured as a dbt source in:

`models/source.yml`

The source references:

`DEV.RAW.WEATHER_DATA`

---

## dbt Transform Model

The staging model is:

`models/transform/stg_weather_data.sql`

It reads from the raw Snowflake source using:

```text
{{ source('raw', 'weather_data') }}
```

The model creates the derived field:

`AVG_TEMP`

using:

```text
(TEMP_MAX + TEMP_MIN) / 2
```

This staging model provides a cleaned and simplified version of the raw weather data.

---

## dbt Analytics Model

The analytics model is:

`models/analytics/weather_metrics.sql`

It reads from the staging model using:

```text
{{ ref('stg_weather_data') }}
```

The analytics model creates the following metrics:

- `MOVING_AVG_7D`
- `ROLLING_RAINFALL_7D`
- `TEMPERATURE_ANOMALY`

The final analytics dataset is:

`DEV.ANALYTICS.WEATHER_METRICS`

---

## Weather Metrics

### 7-Day Moving Average Temperature

`MOVING_AVG_7D`

Calculates the average temperature for the current day and previous six days for each city.

### 7-Day Rolling Rainfall

`ROLLING_RAINFALL_7D`

Calculates total precipitation for the current day and previous six days for each city.

### Temperature Anomaly

`TEMPERATURE_ANOMALY`

Compares each day's average temperature with the overall average temperature for that city.

---

## Final Analytics Dataset

The main analytics dataset is:

`DEV.ANALYTICS.WEATHER_METRICS`

Important fields include:

- `CITY`
- `DATE`
- `AVG_TEMP`
- `MOVING_AVG_7D`
- `ROLLING_RAINFALL_7D`
- `TEMPERATURE_ANOMALY`

The dataset contains records for both San Jose and San Francisco.

---

## dbt Tests

Data quality tests are defined in:

`models/schema.yml`

The project includes:

- `not_null`
- `accepted_values`

Tests are applied to important fields including:

- `CITY`
- `DATE`
- `AVG_TEMP`
- `MOVING_AVG_7D`
- `ROLLING_RAINFALL_7D`
- `TEMPERATURE_ANOMALY`

The `CITY` field is restricted to:

- San Jose
- San Francisco

---

## Custom Duplicate Test

A custom dbt test is located at:

`tests/no_duplicate_city_date.sql`

This test verifies that duplicate combinations of:

`CITY + DATE`

do not exist.

A successful test returns zero rows.

---

## dbt Snapshot

The snapshot definition is located at:

`snapshots/weather_snapshot.sql`

The snapshot uses the `check` strategy.

It tracks changes in:

- `TEMP_MAX`
- `TEMP_MIN`
- `PRECIPITATION`
- `WEATHER_CODE`

The unique key is based on:

`CITY + DATE`

The snapshot output is stored in:

`DEV.ANALYTICS.WEATHER_SNAPSHOT`

dbt automatically adds metadata fields including:

- `DBT_SCD_ID`
- `DBT_UPDATED_AT`
- `DBT_VALID_FROM`
- `DBT_VALID_TO`

---

## dbt Commands

The main dbt commands used in this project are:

```bash
dbt run
```

```bash
dbt test
```

```bash
dbt snapshot
```

The same dbt operations are also executed through the Airflow DAG.

---

## Airflow and dbt Integration

After weather data is loaded into Snowflake, Airflow executes the dbt workflow.

```text
load_weather
     |
     v
dbt_run
     |
     v
dbt_test
     |
     v
dbt_snapshot
```

This ensures that dbt transformations run only after the ETL load completes successfully.

---

## Dashboard

Preset / Apache Superset is used as the BI visualization tool.

The dashboard uses:

`DEV.ANALYTICS.WEATHER_METRICS`

as the source dataset.

Planned visualizations include:

- 7-Day Moving Average Temperature
- 7-Day Rolling Rainfall
- Temperature Anomaly
- San Jose vs. San Francisco weather comparison

Dashboard filters can include:

- City
- Date

---

## Project Setup

### 1. Clone the Repository

```bash
git clone https://github.com/shreyakaushik11/weather-prediction-analytics.git
cd weather-prediction-analytics
```

---

### 2. Generate Snowflake RSA Keys

Each team member should create their own RSA key pair.

Check that OpenSSL is installed:

```bash
openssl version
```

Generate the encrypted private key:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 des3 -inform PEM -out rsa_key.p8
```

Generate the public key:

```bash
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

The private key should be stored locally at:

`keys/rsa_key.p8`

The private key must not be committed to GitHub.

---

## Snowflake Key-Pair Authentication

The public key should be associated with the user's Snowflake account.

The private key is used by Airflow and dbt for authentication.

Inside the Docker container, the private key is available at:

`/opt/airflow/keys/rsa_key.p8`

Each team member should use their own Snowflake account credentials and key pair.

---

## Start the Docker Environment

Run:

```bash
docker compose up -d
```

Docker starts the Airflow services and mounts the project folders into the Airflow container.

---

## Configure the Airflow Snowflake Connection

Create the following Snowflake connection in Airflow:

```text
Connection ID: snowflake_conn
Connection Type: Snowflake
Database: DEV
Schema: RAW
Warehouse: COMPUTE_WH
```

The account, user, private key, and private-key passphrase should use the credentials of the individual team member.

---

## Configure dbt

Each team member must configure their own local dbt profile.

The dbt configuration should use:

```text
Database: DEV
Schema: ANALYTICS
Warehouse: COMPUTE_WH
```

The private key path inside Docker is:

`/opt/airflow/keys/rsa_key.p8`

Validate the dbt configuration using:

```bash
dbt debug
```

Then the dbt project can be tested manually with:

```bash
dbt run
dbt test
dbt snapshot
```

---

## Run the Pipeline

Open Airflow and trigger:

`weather_etl`

A successful pipeline run executes:

```text
extract_weather
extract_weather_1
load_weather
dbt_run
dbt_test
dbt_snapshot
```

---

## Repository Structure

```text
weather-prediction-analytics/
|
|-- dags/
|   `-- weather_etl.py
|
|-- dbt/
|   `-- weather_analytics/
|       |
|       |-- models/
|       |   |
|       |   |-- transform/
|       |   |   `-- stg_weather_data.sql
|       |   |
|       |   |-- analytics/
|       |   |   `-- weather_metrics.sql
|       |   |
|       |   |-- source.yml
|       |   `-- schema.yml
|       |
|       |-- snapshots/
|       |   `-- weather_snapshot.sql
|       |
|       |-- tests/
|       |   `-- no_duplicate_city_date.sql
|       |
|       `-- dbt_project.yml
|
|-- keys/
|-- config/
|-- logs/
|-- plugins/
|-- docker-compose.yaml
|-- .gitignore
`-- README.md
```

---

## Security

Sensitive credentials must not be committed to the repository.

The following should remain local:

- `rsa_key.p8`
- `*.pem`
- `profiles.yml`
- `.env`

Do not commit:

- Snowflake passwords
- RSA private keys
- RSA private-key passphrases
- dbt credentials
- other sensitive connection information

---

## Team Members

- Shreya Kaushik
- Tara Lamb

---

## Future Work

Possible improvements include:

- adding more cities
- adding additional weather metrics
- scheduling the Airflow DAG to run automatically
- adding more dbt data-quality tests
- analyzing longer historical periods
- expanding snapshot analysis
- adding interactive dashboard filters
- comparing weather trends across more locations

---

## GitHub Repository

https://github.com/shreyakaushik11/weather-prediction-analytics