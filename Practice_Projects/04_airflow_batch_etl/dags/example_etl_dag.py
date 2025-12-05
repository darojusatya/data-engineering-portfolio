from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {'owner':'airflow','retries':1,'retry_delay':timedelta(minutes=5)}
with DAG('example_etl', start_date=datetime(2025,1,1), schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    t1 = BashOperator(task_id='extract_api', bash_command='python /opt/airflow/scripts/extract_api.py')
    t2 = BashOperator(task_id='transform_spark', bash_command='python /opt/airflow/scripts/transform_spark.py')
    t3 = BashOperator(task_id='load_postgres', bash_command='python /opt/airflow/scripts/load_postgres.py')
    t1 >> t2 >> t3
