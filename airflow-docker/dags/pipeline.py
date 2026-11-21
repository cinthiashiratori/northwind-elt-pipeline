from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta

TASKS_DIR = "/opt/airflow/dags/tasks"
DATE_MACRO = "{{ ds | default(macros.datetime.utcnow().strftime('%Y-%m-%d')) }}"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="northwind_pipeline",
    default_args=default_args,
    description="Extrai Northwind + CSV para disco local e carrega no PostgreSQL final",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["northwind", "meltano", "indicium"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    extract_postgres = BashOperator(
        task_id="extract_postgres",
        bash_command=f"bash {TASKS_DIR}/extract_postgres.sh {DATE_MACRO}",
    )

    extract_csv = BashOperator(
        task_id="extract_csv",
        bash_command=f"bash {TASKS_DIR}/extract_csv.sh {DATE_MACRO}",
    )

    load_to_final_db = BashOperator(
        task_id="load_to_final_db",
        bash_command=f"python {TASKS_DIR}/load_to_final_db.py {DATE_MACRO}",
    )

    start >> [extract_postgres, extract_csv] >> load_to_final_db >> end
