from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
import csv
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import pymssql

mssql_connect = pymssql.connect(
    server='sql2017a',
    user='CMX\powerbi_sa',
    password='17epHo39UjIqa4itiqlY',
    database='PWA_Content'
)

pg_connect = psycopg2.connect(user="postgres",
                              password="postgres",
                              host="resman-02.vm.cmx.ru",
                              port="5432",
                              database="BI")

engine = create_engine('postgresql://postgres:postgres@resman-02.vm.cmx.ru:5432/BI')


def upload_data(table_name, df):
    num_rows = len(df)
    chunk_size = num_rows/5
    with engine.connect() as conn:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size - 1, num_rows)
            print(f"loading rows {start}-{end}")
            df.loc[start: end].to_sql(table_name, con=conn, schema='project_stg', if_exists='append', index=False)
            start = end + 1
        print('load complete')


def update_facts_stg():
    columns = ['task_name', 'task_start_date', 'task_finish_date', 'arf_code', 'time_by_day', 'pr_comment',
               'task_initiator',
               'timesheet_status_name', 'timesheet_line_status_name', 'planned_work', 'actual_work_billable',
               'assignment_actual_work',
               'actual_overtime_work_billable', 'assignment_actual_overtime_work', 'project_uid', 'task_uid',
               'timesheet_line_class_name',
               'timesheet_line_class_type', 'timesheet_line_status', 'timesheet_status_id',
               'actual_overtime_work_billable_orig',
               'actual_work_billable_orig', 'actual_work_non_billable', 'actual_overtime_work_non_billable',
               'assignment_uid',
               'resource_uid', 'timesheet_line_uid', 'modified_date']
    cursor = mssql_connect.cursor()
    cursor.execute("""SELECT
        [TaskName] as task_name
        ,[TaskStartDate]as task_start_date
        ,[TaskFinishDate]as task_finish_date
        ,REPLACE([ARFCode], nchar(0160), '') as arf_code
        ,[TimeByDay] as time_by_day
        ,[Comment] as pr_comment
        ,[TaskInitiator] as task_initiator
        ,[TimesheetStatusName] as timesheet_status_name               
        ,[TimesheetLineStatusName] as timesheet_line_status_name
        ,[PlannedWork] as planned_work
        ,[ActualWorkBillable] as actual_work_billable
        ,[AssignmentActualWork]as assignment_actual_work
        ,[ActualOvertimeWorkBillable] as actual_overtime_work_billable
        ,[AssignmentActualOvertimeWork] as assignment_actual_overtime_work
        ,[ProjectUID] as project_uid
        ,[taskUID] as task_uid
        ,[TimesheetLineClassName] as timesheet_line_class_name
        ,[TimesheetLineClassType] as timesheet_line_class_type
        ,[TimesheetLineStatus] as timesheet_line_status
        ,[TimesheetStatusID] as timesheet_status_id
        ,[ActualOvertimeWorkBillable_orig] as actual_overtime_work_billable_orig
        ,[ActualWorkBillable_orig] as actual_work_billable_orig
        ,[ActualOvertimeWorkNonBillable]as actual_work_non_billable 
        ,[ActualWorkNonBillable] as actual_overtime_work_non_billable
        ,[AssignmentUID] as assignment_uid
        ,[ResourceUID] as resource_uid  
        ,[TimesheetLineUID] as timesheet_line_uid
        ,[ModifiedDate] as modified_date
    FROM [PWA_CustomReports].[dbo].[ActualWork4ARF_with_PT_v1]""")
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    cursor.close()
    mssql_connect.close()

    cursor = pg_connect.cursor()

    postgres_insert_query = """TRUNCATE TABLE project_stg.facts"""
    cursor.execute(postgres_insert_query)
    pg_connect.commit()
    pg_connect.close()

    upload_data('facts', df)


with DAG(
        dag_id='update_project_stg',
        schedule_interval='@daily',
        catchup=False,
        start_date=datetime(2022, 7, 1),
        tags=['update', 'project', 'sql', 'stg'],
        template_searchpath='/opt/airflow/dags/internal-bi-project'
) as dag:
    update_departments = PythonOperator(task_id='update_facts_stg',
                                        python_callable=update_facts_stg)

update_departments
