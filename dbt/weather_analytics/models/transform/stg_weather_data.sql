select
    city,
    latitude,
    longitude,
    date,
    temp_max,
    temp_min,
    precipitation,
    weather_code,
    (temp_max + temp_min) / 2 as avg_temp
from {{ source('raw', 'weather_data') }}