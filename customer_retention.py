import time
import requests
import json
import pandas as pd
import logging

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

task_logger = logging.getLogger('airflow.task')

HTTP_CREDENTIALS = HttpHook.get_connection('http_conn_id')
API_KEY = HTTP_CREDENTIALS.extra_dejson.get('api_key')
API_BASE_URL = HTTP_CREDENTIALS.host
S3_BASE_URL = 'https://storage.yandexcloud.net/s3-sprint3'

POSTGRES_CONN_ID = 'postgresql_de'

NICKNAME = 'cheburek'
COHORT = '666'

HEADERS = {
    'X-Nickname': NICKNAME,
    'X-Cohort': COHORT,
    'X-API-KEY': API_KEY,
    'X-Project': 'True',
    'Content-Type': 'application/x-www-form-urlencoded'
}


def generate_report(ti):
    task_logger.info('Making request generate_report')

    response = requests.post(f'{API_BASE_URL}/generate_report', headers=HEADERS)
    response.raise_for_status()
    task_id = json.loads(response.content).get('task_id')
    ti.xcom_push(key='task_id', value=task_id)
    task_logger.info(f'Response is {response.content}')


def get_report(ti):
    task_logger.info('Making request get_report')

    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{API_BASE_URL}/get_report?task_id={task_id}', headers=HEADERS)
        response.raise_for_status()
        task_logger.info(f'Response is {response.content}')
        status = json.loads(response.content).get('status')
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(35)

    if not report_id:
        raise TimeoutError('Could not get a report_id from API')

    ti.xcom_push(key='report_id', value=report_id)
    task_logger.info(f'Report_id={report_id}')


def get_increment(date, ti):
    task_logger.info('Making request get_increment')

    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{API_BASE_URL}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=HEADERS)
    response.raise_for_status()
    task_logger.info(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    ti.xcom_push(key='increment_id', value=increment_id)
    task_logger.info(f'increment_id={increment_id}')


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    task_logger.info(f'Uploading file {filename} to {pg_schema}.{pg_table}')

    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'{S3_BASE_URL}/cohort_{COHORT}/{NICKNAME}/project/{increment_id}/{filename}'

    local_filename = date.replace('-', '') + '_' + filename

    response = requests.get(s3_filename)

    with open(f"{local_filename}", "wb") as file:
        file.write(response.content)

    df = pd.read_csv(local_filename)
    df.drop_duplicates(subset=['id'])

    postgres_hook = PostgresHook(POSTGRES_CONN_ID)
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)


ARGS = {
    "owner": NICKNAME,
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# deafault Airflow's template for the DAG's execution date
# see more at: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
business_dt = '{{ ds }}'

with DAG(
        'customer_retention',
        default_args=ARGS,
        description='Provide default dag for sprint3',
        catchup=True,
        start_date=datetime.today() - timedelta(days=8),
        end_date=datetime.today() - timedelta(days=1),
) as dag:
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

    upload_user_order_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_orders_log_inc.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'})

    dim_marts = [
        'd_item',
        'd_customer',
        'd_city',
    ]

    marts_tasks = []

    for mart_name in dim_marts:
        marts_tasks.append(
            PostgresOperator(
                task_id=f'update_{mart_name}',
                sql=f"sql/mart.{mart_name}.sql",
                postgres_conn_id=POSTGRES_CONN_ID,
            )
        )

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        sql="sql/mart.f_sales.sql",
        postgres_conn_id=POSTGRES_CONN_ID,
    )

    (
            generate_report
            >> get_report
            >> get_increment
            >> upload_user_order_inc
            >> marts_tasks
            >> update_f_sales
    )

