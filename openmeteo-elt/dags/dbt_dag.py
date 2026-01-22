from datetime import datetime
from pathlib import Path
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig

# The path to the dbt project
dbt_project_path = Path("/opt/airflow/dbt")

profile_config = ProfileConfig(
    profile_name="weather_elt",
    target_name="dev",
    profiles_yml_filepath=dbt_project_path / "profiles.yml"
)

dbt_dag = DbtDag(
    project_config=ProjectConfig(dbt_project_path),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="dbt",
    ),
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    dag_id="dbt_weather_transformation",
    tags=["dbt", "weather"],
)
