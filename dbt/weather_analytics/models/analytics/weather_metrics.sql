with weather as (

    select *
    from {{ ref('stg_weather_data') }}

),

metrics as (

    select
        city,
        latitude,
        longitude,
        date,
        temp_max,
        temp_min,
        avg_temp,
        precipitation,
        weather_code,

        avg(avg_temp) over (
            partition by city
            order by date
            rows between 6 preceding and current row
        ) as moving_avg_7d,

        sum(precipitation) over (
            partition by city
            order by date
            rows between 6 preceding and current row
        ) as rolling_rainfall_7d,

        avg_temp - avg(avg_temp) over (
            partition by city
        ) as temperature_anomaly

    from weather

)

select *
from metrics