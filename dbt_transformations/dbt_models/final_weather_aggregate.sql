with daily_weather as (
    select
        Location,
        date_trunc('day', reading_time) as day,
        avg(Temperature) as avg_temp,
        avg(Pressure) as avg_pressure,
        avg(Humidity) as avg_humidity,
        avg("Wind Speed") as avg_wind_speed
    from
        {{ ref('staging_weather') }}
    group by
        Location, day
)
select
    *
from
    daily_weather