from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator



with DAG(
    dag_id='insert_rate_test',
    schedule_interval='@hourly',
    catchup=False,
    start_date=datetime(2022, 7, 1),
    tags=['update', 'tempus', 'sql'],
    template_searchpath='/opt/airflow/dags/internal-bi-project'
) as dag:
    query = PostgresOperator(
        task_id='insert_rate_test',
        postgres_conn_id='bi',
        sql='sql/insert.sql'
    )
