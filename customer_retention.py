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


NICKNAME = 'AlexSaykov'
COHORT = '666'


def print_1(ti):
    task_logger.info('print1')
    task_logger.info('{{ ds }}')


def print_2(ti):
    task_logger.info('print2')
    
def print_3(ti):
    task_logger.info('print2')



ARGS = {
    "owner": NICKNAME,
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}


business_dt = '{{ ds }}'

with DAG(
        'simpleDagFromAlex',
        default_args=ARGS,
        description='simpleDagFromAlex',
        catchup=True,
        start_date=datetime(2022, 8, 8),
        end_date=datetime(2023, 7, 1),
) as dag:
    print1_1 = PythonOperator(
        task_id='print_1',
        python_callable=print_1)

    print1_2 = PythonOperator(
        task_id='print_2',
        python_callable=print_2)
    
    print1_3 = PythonOperator(
        task_id='print_3',
        python_callable=print_3)

 
    (
            print1_1 >> print1_2 >> print1_3

    )

