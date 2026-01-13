import asyncio
from pathlib import Path
import httpx
import pandas as pd
from sqlalchemy import create_engine
import datetime

async def fetch_weather(
    client: httpx.AsyncClient,
    city: str,
    latitude: float,
    longitude: float,
    semaphore: asyncio.Semaphore,
    start_timestamp: int = None,
    end_timestamp: int = None,
):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,relativehumidity_2m,windspeed_10m",
    }
    if start_timestamp and end_timestamp:
        params["start_date"] = start_timestamp
        params["end_date"] = end_timestamp
        params["timezone"] = "auto"
    else:
        # If no timestamps are provided, fetch for the current day
        today = datetime.date.today()
        params["start_date"] = today.isoformat()
        params["end_date"] = today.isoformat()
        params["timezone"] = "auto"

    async with semaphore:
        print(f"Requesting weather for {city}...")
        response = await client.get(url, params=params)
        if response.status_code == 429:
            print(f"Rate limited for {city}, waiting 2 seconds...")
            await asyncio.sleep(2)
            response = await client.get(url, params=params)

        response.raise_for_status()
        data = response.json()
        data["city"] = city

    print(f"Received data for {city}")
    return data


def transform_to_dataframe(data: dict, output_dir: Path, db_engine):
    """
    Transforms a single weather data dictionary into a pandas DataFrame,
    saves it to an individual CSV, appends it to a combined CSV,
    and writes it to the PostgreSQL database.
    """
    hourly_data = data.get("hourly", {})
    hourly_units = data.get("hourly_units", {})
    city = data.get("city")

    if not hourly_data:
        return

    df = pd.DataFrame(hourly_data)

    # --- Database Write ---
    # Create a copy for SQL with correct column names and types
    df_sql = df.copy()
    df_sql["city"] = city
    df_sql["time"] = pd.to_datetime(df_sql["time"])

    # Filter to ensure only schema columns are present
    expected_cols = ["city", "time", "temperature_2m", "relativehumidity_2m", "windspeed_10m"]
    # Verify columns exist before selecting to avoid KeyError
    available_cols = [c for c in expected_cols if c in df_sql.columns]
    df_sql = df_sql[available_cols]

    try:
        df_sql.to_sql("weather_data", db_engine, if_exists="append", index=False)
        print(f"Written data for {city} to database.")
    except Exception as e:
        print(f"Failed to write to DB for {city}: {e}")

    # --- CSV Write ---
    # Rename columns to include units
    rename_map = {}
    for col in df.columns:
        if col in hourly_units:
            rename_map[col] = f"{col} ({hourly_units[col]})"

    df = df.rename(columns=rename_map)
    df["city"] = city

    # Move 'city' column to the front
    cols = ["city"] + [c for c in df.columns if c != "city"]
    df = df[cols]

    # Save individual CSV
    city_name_safe = city.lower().replace(" ", "_")
    output_file = output_dir / f"{city_name_safe}_weather.csv"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"Saved {output_file}")

    # Append to combined CSV
    combined_file = output_dir / "all_cities_weather.csv"
    header = not combined_file.exists()
    df.to_csv(combined_file, mode="a", header=header, index=False)


async def fetch_and_save(
    client: httpx.AsyncClient,
    city: str,
    lat: float,
    lon: float,
    semaphore: asyncio.Semaphore,
    output_dir: Path,
    db_engine,
):
    data = await fetch_weather(client, city, lat, lon, semaphore)
    transform_to_dataframe(data, output_dir, db_engine)
