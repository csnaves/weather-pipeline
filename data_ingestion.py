import argparse
import json
import os
import tempfile
from datetime import datetime, timezone

import openmeteo_requests
from openmeteo_sdk.WeatherApiResponse import WeatherApiResponse
import requests
import pandas as pd
import requests_cache
from retry_requests import retry
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

LOCATIONS = [
    ("Atlanta", "Georgia"),
    ("New York", "New York"),
    ("Washington", "DC"),
    ("San Francisco", "CA"),
    "Daniel Boone National Forest, USA",
]


def get_bbox(location):
    """Get bounding box from Nominatim for a (city, state) tuple or free-form string."""
    url = "https://nominatim.openstreetmap.org/search"
    headers = {"User-Agent": "weather-pipeline"}

    if isinstance(location, tuple):
        city, state = location
        params = {"city": city, "state": state, "format": "json", "limit": 1}
    else:
        params = {"q": location, "format": "json", "limit": 1}

    resp = requests.get(url, params=params, headers=headers)
    result = resp.json()[0]
    bbox = result["boundingbox"]
    return {
        "south_lat": bbox[0],
        "north_lat": bbox[1],
        "west_lon": bbox[2],
        "east_lon": bbox[3],
    }


def _get_client():
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)


def get_grid_points(box):
    """Resolve bounding box to a list of grid lat/lon points via the Best Match model."""
    openmeteo = _get_client()
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "bounding_box": f"{box['south_lat']},{box['west_lon']},{box['north_lat']},{box['east_lon']}",
        "models": "icon_global",
        "forecast_hours": 0,
    }
    responses = openmeteo.weather_api(url, params=params)
    latitudes = [r.Latitude() for r in responses]
    longitudes = [r.Longitude() for r in responses]
    return latitudes, longitudes


def get_weather(latitudes, longitudes, mode="history"):
    """Fetch weather data for a list of lat/lon points using the Best Match model.

    mode="history"  -> past 24 hours
    mode="forecast" -> next 1 hour
    """
    openmeteo = _get_client()
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitudes,
        "longitude": longitudes,
        "hourly": ["temperature_2m", "is_day", "precipitation_probability", "precipitation"],
        "wind_speed_unit": "mph",
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
    }

    if mode == "history":
        params["past_hours"] = 24
        params["forecast_hours"] = 0
    else:
        params["past_hours"] = 0
        params["forecast_hours"] = 1

    responses = openmeteo.weather_api(url, params=params)
    return responses


def parse_responses(responses: list[WeatherApiResponse], location_label):
    """Parse Open-Meteo responses into a list of DataFrames."""
    dataframes = []
    for response in responses:
        hourly = response.Hourly()
        hourly_data = {
            "timestamp": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            ),
            "location": location_label,
            "latitude": response.Latitude(),
            "longitude": response.Longitude(),
            "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
            "is_day": hourly.Variables(1).ValuesAsNumpy(),
            "precipitation_probability": hourly.Variables(2).ValuesAsNumpy(),
            "precipitation": hourly.Variables(3).ValuesAsNumpy(),
        }
        dataframes.append(pd.DataFrame(data=hourly_data))
    return dataframes


def get_snowflake_connection():
    """Create a Snowflake connection from environment variables."""
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    )


def responses_to_json(dataframes: list[pd.DataFrame], location_label, mode):
    """Serialize DataFrames to an NDJSON file (one JSON object per line), returns the file path."""
    safe_label = location_label.replace(" ", "_").replace(",", "")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{safe_label}_{mode}_{timestamp}.json"
    filepath = os.path.join(tempfile.gettempdir(), filename)

    with open(filepath, "w") as f:
        for df in dataframes:
            df_serializable = df.copy()
            df_serializable["timestamp"] = df_serializable["timestamp"].astype(str)
            for record in df_serializable.to_dict(orient="records"):
                record["mode"] = mode
                f.write(json.dumps(record) + "\n")

    return filepath


def upload_to_snowflake(connection, local_path):
    """PUT a local JSON file to the raw_weather table stage and COPY INTO the table."""
    cur = connection.cursor()
    try:
        cur.execute(f"PUT 'file://{local_path}' @%raw_weather AUTO_COMPRESS=TRUE")
        # Use metadata start scan time for an accurate time value of record loading
        cur.execute(
            "COPY INTO raw_weather(raw, loaded_at) "
            "FROM (SELECT $1, METADATA$START_SCAN_TIME FROM @%raw_weather) "
            "FILE_FORMAT = (TYPE = 'JSON')"
        )
    finally:
        cur.close()


def ingest(mode, locations=None, dry_run=False):
    """Run ingestion for given locations, falling back to LOCATIONS."""
    if locations is None:
        locations = LOCATIONS

    conn = None
    if not dry_run:
        conn = get_snowflake_connection()

    try:
        for location in locations:
            label = location if isinstance(location, str) else f"{location[0]}, {location[1]}"
            bbox = get_bbox(location)
            latitudes, longitudes = get_grid_points(bbox)
            print(f"\n{'='*60}")
            print(f"Location: {label} | Mode: {mode}")
            print(f"Bounding Box: {bbox}")
            print(f"Grid Points: {len(latitudes)}")
            print(f"{'='*60}")

            responses = get_weather(latitudes, longitudes, mode=mode)
            dataframes = parse_responses(responses, label)

            for df in dataframes:
                df: pd.DataFrame
                lat = df["latitude"].iloc[0]
                lon = df["longitude"].iloc[0]
                print(f"\nCoordinates: {lat}°N {lon}°E")
                print(df.to_string(index=False))

            json_path = responses_to_json(dataframes, label, mode)
            print(f"\nJSON written to: {json_path}")

            if not dry_run:
                upload_to_snowflake(conn, json_path)
                print(f"Uploaded to Snowflake @%raw_weather")

            os.remove(json_path)
    finally:
        if conn:
            conn.close()


def parse_location_arg(value):
    """Parse a location argument: 'city,state' becomes a tuple, anything else stays a string."""
    if "," in value:
        parts = [p.strip() for p in value.split(",", 1)]
        if len(parts) == 2:
            return tuple(parts)
    return value


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather data ingestion pipeline")
    parser.add_argument(
        "--mode",
        choices=["history", "forecast"],
        required=True,
        help="history: 24h backfill | forecast: next 1h (for cron)",
    )
    parser.add_argument(
        "--location",
        type=parse_location_arg,
        action="append",
        help="Location to query. Use 'City, State' or a free-form name. Can be repeated. "
             "If omitted, uses the default LOCATIONS list.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip Snowflake upload, just fetch and print data.",
    )
    args = parser.parse_args()
    ingest(args.mode, locations=args.location, dry_run=args.dry_run)
