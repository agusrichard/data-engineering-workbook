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
    assert "extract_weather_data" in dag.task_ids
    
    # Check TaskGroup exists
    assert "transform_weather_data" in dag.task_group_dict
    
    # Check dependencies
    extract_task = dag.get_task("extract_weather_data")
    
    # Get all tasks that belong to the transform group
    transform_group_task_ids = [
        t_id for t_id in dag.task_ids 
        if t_id.startswith("transform_weather_data.")
    ]
    assert len(transform_group_task_ids) > 0, "No tasks found in transform_weather_data group"
    
    # Verify extract_task has downstream tasks
    assert len(extract_task.downstream_task_ids) > 0, "extract_weather_data has no downstream tasks"
    
    # Verify at least one downstream task is in the transform group
    # Cosmos might link via setup/teardown or direct task links.
    # We check if any downstream task is part of the group.
    downstream_ids = extract_task.downstream_task_ids
    is_connected = any(d_id in transform_group_task_ids for d_id in downstream_ids)
    
    assert is_connected, f"extract_weather_data is not connected to transform dictionary. Downstream: {downstream_ids}"

def test_weather_elt_dag_config(dagbag):
    """Test specific configuration of the weather_elt DAG."""
    dag_id = "weather_elt"
    dag = dagbag.get_dag(dag_id)
    
    # Airflow 3 uses 'schedule'
    schedule = getattr(dag, "schedule", None)
    if schedule is None and hasattr(dag, "schedule_interval"):
        schedule = dag.schedule_interval
        
    assert schedule == "@daily"
    assert dag.catchup is False
    assert "weather" in dag.tags
    assert dag.default_args["retries"] == 2
