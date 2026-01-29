import pytest
from airflow.models import DagBag

@pytest.fixture()
def dagbag():
    return DagBag(include_examples=False)

def test_weather_elt_dag_integrity(dagbag):
    """Test that the DAG loads without import errors."""
    dag_id = "weather_elt"
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG '{dag_id}' not found in DagBag"
    assert len(dagbag.import_errors) == 0, f"Import errors found: {dagbag.import_errors}"

def test_weather_elt_dag_structure(dagbag):
    """Test the structure and dependencies of the weather_elt DAG."""
    dag_id = "weather_elt"
    dag = dagbag.get_dag(dag_id)
    
    # Check tasks exist
    task_ids = dag.task_ids
    assert "extract_weather_data" in task_ids
    assert "transform_weather_data" in task_ids
    
    # Check dependencies
    extract_task = dag.get_task("extract_weather_data")
    transform_task = dag.get_task("transform_weather_data")
    
    assert transform_task in extract_task.downstream_list
    assert extract_task in transform_task.upstream_list

def test_weather_elt_dag_config(dagbag):
    """Test specific configuration of the weather_elt DAG."""
    dag_id = "weather_elt"
    dag = dagbag.get_dag(dag_id)
    
    assert dag.schedule_interval == "@daily"
    assert dag.catchup is False
    assert "weather" in dag.tags
    assert dag.default_args["retries"] == 2
