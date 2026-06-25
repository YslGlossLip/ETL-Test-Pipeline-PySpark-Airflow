from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


PROJECT_DIR = "/opt/airflow/project"


with DAG(
    dag_id="etl-pipeline-test",
    description="Build daily marts with PySpark",
    start_date=datetime(2021, 7, 21),
    end_date=datetime(2021, 8, 9),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["etl","@daily","pyspark", "test-task"]
) as dag:
    start = EmptyOperator(task_id="start")

    check_input_files = BashOperator(
        task_id="check_input_files",
        bash_command=(
        f"test -f {PROJECT_DIR}/data/input/interview.X.csv && "
        f"test -f {PROJECT_DIR}/data/input/interview.y.csv"
        )      
    )

    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command=(
        f"cd {PROJECT_DIR} && "
        f"python jobs/build_daily_marts.py --process-date {{{{ ds }}}}"
        )
    )

    end = EmptyOperator(task_id="end")

    start >> check_input_files >> run_spark_job >> end
