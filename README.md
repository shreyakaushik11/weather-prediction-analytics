# Weather Prediction Analytics

## Project Overview

This project implements an end-to-end weather analytics pipeline using Apache Airflow, Snowflake, dbt, and Preset / Apache Superset.

The pipeline collects weather data for San Jose and San Francisco from the Open-Meteo API, loads the raw data into Snowflake, transforms the data using dbt, calculates analytical weather metrics, and prepares the final dataset for dashboard visualization.

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