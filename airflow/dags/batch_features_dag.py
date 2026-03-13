from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'batch_features_materialization',
    default_args=default_args,
    description='Compute batch features and materialize to Feast online store',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['features', 'spark', 'feast'],
) as dag:

    # Task 1: Run Spark Job to compute batch features and write to Postgres
    # (Since we are in a lightweight Docker compose, we just run the Python script locally)
    compute_batch_features = BashOperator(
        task_id='compute_batch_features',
        bash_command='python /opt/spark-jobs/batch_features.py',
    )

    # Task 2: Materialize features from Postgres (offline) to Redis (online)
    materialize_feast = BashOperator(
        task_id='materialize_feast',
        bash_command='cd /opt/feast/feature_repo && feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")',
    )

    compute_batch_features >> materialize_feast
