from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime
import requests

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract_weather(city, latitude, longitude):
    url = Variable.get("weather_api_url")

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "forecast_days": 1,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code"
        ]
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

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

    print(f"City: {city}")
    print(f"Number of records: {len(records)}")
    print("First 5 records:")

    for record in records[:5]:
        print(record)

    return records


@task
def load_weather(san_jose_records, san_francisco_records):
    cur = return_snowflake_conn()

    target_table = "raw.weather_data"

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            city VARCHAR,
            latitude FLOAT,
            longitude FLOAT,
            date DATE,
            temp_max FLOAT,
            temp_min FLOAT,
            precipitation FLOAT,
            weather_code INTEGER,
            PRIMARY KEY (city, date)
        );
    """)

    records = san_jose_records + san_francisco_records

    try:
        cur.execute("BEGIN;")

        cur.execute(f"DELETE FROM {target_table};")

        for r in records:
            sql = f"""
                INSERT INTO {target_table} (
                    city,
                    latitude,
                    longitude,
                    date,
                    temp_max,
                    temp_min,
                    precipitation,
                    weather_code
                )
                VALUES (
                    '{r[0]}',
                    {r[1]},
                    {r[2]},
                    '{r[3]}',
                    {r[4]},
                    {r[5]},
                    {r[6]},
                    {r[7]}
                );
            """

            cur.execute(sql)

        cur.execute("COMMIT;")

        print(f"Successfully loaded {len(records)} records into {target_table}")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Error loading weather data: {e}")
        raise


with DAG(
    dag_id="weather_etl",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    schedule=None
) as dag:

    san_jose_latitude = float(Variable.get("san_jose_latitude"))
    san_jose_longitude = float(Variable.get("san_jose_longitude"))

    san_francisco_latitude = float(
        Variable.get("san_francisco_latitude")
    )
    san_francisco_longitude = float(
        Variable.get("san_francisco_longitude")
    )

    san_jose_weather = extract_weather(
        "San Jose",
        san_jose_latitude,
        san_jose_longitude
    )

    san_francisco_weather = extract_weather(
        "San Francisco",
        san_francisco_latitude,
        san_francisco_longitude
    )

    load_weather(
        san_jose_weather,
        san_francisco_weather
    )