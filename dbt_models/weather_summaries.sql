{{
    config(
        materialized='incremental',
        unique_key=['location', 'forecast_timestamp'],
        incremental_strategy='merge',
        merge_exclude_columns=['created_at']
    )
}}

with source_weather as (
    select 
        location,
        forecast_timestamp,
        temperature,
        precipitation_probability,
        precipitation,
        is_day,
        updated_at as source_updated_at
    from {{ ref('weather_analytics') }}
    {% if is_incremental() %}
        where updated_at > (select max(source_updated_at) from {{ this }})
    {% endif %}

),

hourly_grouped as (
    select
        location,
        forecast_timestamp,
        avg(temperature) as avg_temperature,
        avg(precipitation_probability) as avg_precipitation_probability,
        sum(precipitation) as total_precipitation,
        mode(is_day) as is_day,
        count(*) as grid_point_count,
        max(source_updated_at) as source_updated_at
    from source_weather
    group by location, forecast_timestamp
)

select
    location,
    forecast_timestamp,
    avg_temperature,
    avg_precipitation_probability,
    total_precipitation,
    is_day,
    grid_point_count,
    snowflake.cortex.complete(
        'gemini-3-pro',
        concat(
            'Write a brief 1-2 sentence weather summary for ', location, '. ',
            'Average temperature: ', round(avg_temperature, 1), ' °F. ',
            'Precipitation probability: ', round(avg_precipitation_probability, 0), '%. ',
            'Total precipitation: ', round(total_precipitation, 3), ' inches. ',
            'Time of day: ', iff(is_day, 'daytime', 'nighttime'), '. ',
            'Be concise and conversational.'
        )
    ) as summary,
    source_updated_at,
    current_timestamp() as created_at,
    current_timestamp() as updated_at
from hourly_grouped
