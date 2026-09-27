select
    city,
    date,
    count(*) as record_count

from {{ ref('weather_metrics') }}

group by
    city,
    date

having count(*) > 1