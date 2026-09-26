from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import requests


@task
def extract_weather(city, latitude, longitude):
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code"
        ]
    }

    response = requests.get(url, params=params)
    data = response.json()

    records = []

    for i in range(len(data["daily"]["time"])):
        records.append([
            city,
            latitude,
            longitude,
            data["daily"]["time"][i],
            data["daily"]["temperature_2m_max"][i],
            data["daily"]["temperature_2m_min"][i],
            data["daily"]["precipitation_sum"][i],
            data["daily"]["weather_code"][i]
        ])

    return records


with DAG(
    dag_id="weather_etl",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    schedule=None
) as dag:

    san_jose_weather = extract_weather(
        "San Jose",
        37.3382,
        -121.8863
    )

    san_francisco_weather = extract_weather(
        "San Francisco",
        37.7749,
        -122.4194
    )