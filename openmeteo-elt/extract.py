import asyncio
from pathlib import Path

import httpx
import pandas as pd
from sqlalchemy import create_engine


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
        import datetime

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


async def main():
    # Database connection
    # Using the service name 'postgres' from docker-compose
    db_url = "postgresql://user:password@postgres:5432/weather_db"
    engine = create_engine(db_url)

    # Coordinates for a few cities
    cities = {
        "London": (51.5074, -0.1278),
        "New York": (40.7128, -74.0060),
        "Tokyo": (35.6895, 139.6917),
        "Sydney": (-33.8688, 151.2093),
        "Paris": (48.8566, 2.3522),
        "Berlin": (52.5200, 13.4050),
        "Madrid": (40.4168, -3.7038),
        "Rome": (41.9028, 12.4964),
        "Cairo": (30.0444, 31.2357),
        "Mumbai": (19.0760, 72.8777),
        "Beijing": (39.9042, 116.4074),
        "Sao Paulo": (-23.5505, -46.6333),
        "Mexico City": (19.4326, -99.1332),
        "Los Angeles": (34.0522, -118.2437),
        "Chicago": (41.8781, -87.6298),
        "Moscow": (55.7558, 37.6173),
        "Istanbul": (41.0082, 28.9784),
        "Seoul": (37.5665, 126.9780),
        "Bangkok": (13.7563, 100.5018),
        "Lagos": (6.5244, 3.3792),
    }

    output_dir = Path("csvs")
    if not output_dir.exists():
        print(f"Creating directory: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)


    # Clean up old combined file to avoid appending to previous runs
    combined_file = output_dir / "all_cities_weather.csv"
    if combined_file.exists():
        combined_file.unlink()

    semaphore = asyncio.Semaphore(5)
    async with httpx.AsyncClient() as client:
        tasks = []
        for city, (lat, lon) in cities.items():
            tasks.append(fetch_and_save(client, city, lat, lon, semaphore, output_dir, engine))

        # Run all requests concurrently
        await asyncio.gather(*tasks)

    print(f"\nAll data saved to {output_dir}/ and database.")


if __name__ == "__main__":
    asyncio.run(main())
3