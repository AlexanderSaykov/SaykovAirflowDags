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
        sql="""insert into project.rates_test(resource_uid, start_date, end_date, rate, rate_overtime, sys_update_on)
  values('dcf6bb63-0f08-e911-9218-005056bc1e6d',
             current_date,
             null,
             600,
             1500,
             now())
             on conflict(resource_uid)  do update set

             resource_uid = excluded.resource_uid,
             start_date = excluded.start_date,
             end_date =excluded. end_date,
             rate = excluded.rate,
             rate_overtime = excluded.rate_overtime,
             sys_update_on = excluded.sys_update_on;
 """
    )
