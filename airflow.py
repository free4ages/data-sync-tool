"""
Dynamic DAG generator: for each Pipeline configured in Postgres,
create an Airflow DAG that runs according to its schedule.
Uses PythonVirtualenvOperator to isolate dependencies in a fresh venv.
Assumes:
  - Airflow is configured with CeleryExecutor.
  - A PostgresHook connection `config_db` is defined in Airflow Connections.
  - Celery workers have access to shared filesystem or can pull code from version control.
"""
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonVirtualenvOperator

# Import your pipeline runner
from runner import run_pipeline_by_name


def fetch_pipeline_configs():
    """
    Query Postgres for pipeline definitions, including schedule and requirements.
    """
    hook = PostgresHook(postgres_conn_id='config_db')
    records = hook.get_records(
        "SELECT name, config->>'schedule_interval', config FROM pipelines;"
    )
    pipelines = []
    for name, schedule, config_json in records:
        config = config_json if isinstance(config_json, dict) else json.loads(config_json)
        pipelines.append({
            'name': name,
            'schedule_interval': schedule or '@daily',
            'config': config
        })
    return pipelines

# Load pipeline definitions at import time
pipeline_defs = fetch_pipeline_configs()

for pdef in pipeline_defs:
    dag_id = f"pipeline_{pdef['name']}"
    schedule = pdef['schedule_interval']
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=['dynamic-pipeline'],
    )

    # Factory to capture pipeline_name for the venv task
    def make_runner(name: str):
        def _run():
            run_pipeline_by_name(name)
        return _run

    run_callable = make_runner(pdef['name'])

    # Determine Python package requirements
    requirements = pdef['config'].get('requirements', [
        'nats-py',
        'clickhouse-driver',
        'jinja2',
        # if you replaced glom with get_in helper, remove glom requirement
        'opentelemetry-api',
        'opentelemetry-sdk',
    ])

    # Use Celery queue binding so tasks get executed by CeleryExecutor
    executor_config = {
        "CeleryExecutor": {
            "queue": pdef['config'].get('executor_queue', 'default')
        }
    }

    run_task = PythonVirtualenvOperator(
        task_id='run_pipeline',
        python_callable=run_callable,
        requirements=requirements,
        system_site_packages=False,
        use_dill=True,
        executor_config=executor_config,
        dag=dag,
    )

    globals()[dag_id] = dag