with raw_data as (
    select
        *,
        (Temperature - 273.15) as Temperature_Celsius,
        (to_timestamp(datetime) + interval '1 second' * timezone) as reading_time
    from
        {{ source('raw_weather_data', 'raw_weather_data') }}
)
select
    Location,
    Weather,
    Description,
    Temperature_Celsius as Temperature,
    Pressure,
    Humidity,
    "Wind Speed",
    reading_time
from
    raw_data
