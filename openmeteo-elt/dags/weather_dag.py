import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

# Add include directory to sys.path
sys.path.append(str(Path(__file__).parent.parent / "include"))

from weather_utils import fetch_and_save

# Coordinates for cities
CITIES = {
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

@task
def extract_weather_data(logical_date=None):
    # Setup database connection
    # Try to get connection from Airflow, fallback to default for local dev
    try:
        conn = BaseHook.get_connection("weather_db")
        db_url = conn.get_uri()
    except Exception:
        # Fallback for when connection is not defined yet (first run)
        # Using the service name 'postgres' from docker-compose network
        db_url = "postgresql://airflow:airflow@postgres:5432/weather_db"

    engine = create_engine(db_url)

    output_dir = Path("/tmp/csvs") # Use tmp in Airflow worker
    if output_dir.exists():
        # Cleanup old run
        import shutil
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Use logical_date to fetch data for that specific day
    # Airflow 3.x logical_date is a pendulum object
    execution_date_str = logical_date.to_date_string()
    print(f"Fetching weather data for: {execution_date_str}")

    async def main_loop():
        semaphore = asyncio.Semaphore(5)
        async with httpx.AsyncClient() as client:
            tasks = []
            for city, (lat, lon) in CITIES.items():
                tasks.append(
                    fetch_and_save(
                        client, city, lat, lon, semaphore, output_dir, engine,
                        start_date=execution_date_str,
                        end_date=execution_date_str
                    )
                )

            await asyncio.gather(*tasks)

    asyncio.run(main_loop())
    print(f"Data saved to {output_dir}")


with DAG(
    dag_id="weather_elt",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "weather"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    # Define dbt config
    dbt_project_path = Path("/opt/airflow/dbt")
    profile_config = ProfileConfig(
        profile_name="weather_elt",
        target_name="dev",
        profiles_yml_filepath=dbt_project_path / "profiles.yml"
    )

    transform_weather_data = DbtTaskGroup(
        group_id="transform_weather_data",
        project_config=ProjectConfig(dbt_project_path),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="dbt",
        ),
    )

    extract_weather_data() >> transform_weather_data
