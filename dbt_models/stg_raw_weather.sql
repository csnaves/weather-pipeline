{{
  config(
    materialized = 'view'
  )
}}

with stage as (
    select 
        raw:forecast_timestamp::timestamp_ltz as forecast_timestamp,
        raw:latitude::float as latitude,
        raw:longitude::float as longitude,
        raw:location::string as location,
        raw:is_day::integer::boolean as is_day,
        raw:precipitation::float as precipitation,
        raw:precipitation_probability::float as precipitation_probability,
        raw:temperature_2m::float as temperature,
        raw:ingested_at::timestamp_ltz as ingested_at,
        loaded_at

    from {{ source('public', 'raw_weather') }}
)

select * from stage
qualify row_number() over (partition by forecast_timestamp, latitude, longitude order by loaded_at desc) = 1
