 {{
      config(
          materialized='incremental',
          unique_key=['forecast_timestamp', 'latitude', 'longitude'],
          incremental_strategy = 'merge',
          merge_exclude_columns = ['created_at']
      )
  }}

  SELECT
      forecast_timestamp,
      latitude,
      longitude,
      location,
      temperature,
      is_day,
      precipitation_probability,
      precipitation,
      ingested_at,
      loaded_at,
      current_timestamp() as created_at,
      current_timestamp() as updated_at
  FROM {{ ref('stg_raw_weather') }}

  {% if is_incremental() %}
      WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
  {% endif %}