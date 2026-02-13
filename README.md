# Weather Pipeline

Weather data ingestion pipeline that fetches hourly weather data from [Open-Meteo](https://open-meteo.com/) for configurable locations and loads it into Snowflake for downstream transformation with dbt.

## Architecture

```
Nominatim (geocoding)     Open-Meteo (weather data)
        |                         |
        v                         v
   get_bbox()  -->  get_grid_points()  -->  get_weather()
                                                |
                                                v
                                        parse_responses()
                                                |
                                                v
                                     responses_to_json() (NDJSON)
                                                |
                                                v
                                   PUT  -->  @%raw_weather (table stage)
                                                |
                                                v
                                     COPY INTO raw_weather (VARIANT)
                                                |
                                                v
                                          dbt transforms
```

1. **Geocode** locations via Nominatim to get bounding boxes
2. **Resolve grid points** within each bounding box using `icon_global`
3. **Fetch weather data** for all grid points in a single API call using the Best Match model
4. **Serialize** responses to NDJSON (one JSON object per line, one row per record)
5. **Load** into Snowflake's `raw_weather` table via `PUT` + `COPY INTO`

## Weather Variables

| Variable | Unit |
|---|---|
| `temperature_2m` | Fahrenheit |
| `precipitation` | Inches |
| `precipitation_probability` | Percent |
| `is_day` | 0 (night) / 1 (day) |

## Setup

### Python

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Environment Variables

Copy `.env.example` to `.env` and fill in your Snowflake credentials:

```bash
cp .env.example .env
```

Required variables:
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`
- `SNOWFLAKE_WAREHOUSE`

### Snowflake

Run `setup.sql` in your Snowflake environment to create the raw landing table:

```sql
-- setup.sql
CREATE TABLE IF NOT EXISTS raw_weather (
    raw OBJECT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

## Usage

### Modes

- **`history`** -- Backfills the past 24 hours of weather data
- **`forecast`** -- Fetches the next 2 hours of forecast data (designed for hourly cron)

### Commands

```bash
# 24-hour backfill for all default locations
python data_ingestion.py --mode history

# Hourly forecast for all default locations
python data_ingestion.py --mode forecast

# Specific locations
python data_ingestion.py --mode forecast --location "Denver, Colorado"
python data_ingestion.py --mode forecast --location "Yellowstone National Park"

# Multiple locations
python data_ingestion.py --mode forecast --location "Atlanta, Georgia" --location "Miami, Florida"

# Dry run (skip Snowflake upload)
python data_ingestion.py --mode forecast --dry-run
```

### Cron (hourly forecast)

```cron
0 * * * * cd /path/to/weather-pipeline && venv/bin/python data_ingestion.py --mode forecast
```

## Default Locations

Configured in `LOCATIONS` at the top of `data_ingestion.py`:

```python
LOCATIONS = [
    ("Atlanta", "Georgia"),
    ("New York", "New York"),
    ("Washington", "DC"),
    ("San Francisco", "CA"),
    "Daniel Boone National Forest, USA",
]
```

Locations can be `("City", "State")` tuples or free-form strings for any named area.
