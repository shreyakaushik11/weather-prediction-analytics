{% snapshot weather_snapshot %}

{{
    config(
      target_schema='ANALYTICS',
      unique_key='city || date',
      strategy='check',
      check_cols=['temp_max', 'temp_min', 'precipitation', 'weather_code']
    )
}}

select
    city,
    latitude,
    longitude,
    date,
    temp_max,
    temp_min,
    precipitation,
    weather_code
from {{ source('raw', 'weather_data') }}

{% endsnapshot %}