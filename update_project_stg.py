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

def truncate_data(table_name):
    cursor = pg_connect.cursor()
    postgres_insert_query = f"""TRUNCATE TABLE project_stg.{table_name}"""
    cursor.execute(postgres_insert_query)
    pg_connect.commit()
    pg_connect.close()



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

    truncate_data('facts')
    upload_data('facts', df)


def update_resources_stg():
    columns = ['resource_uid', 'resource_id', 'resource_name', 'rate', 'rate_overtime', 'max_units', 'is_person',
               'resource_type', 'department', 'creation_date', 'hire_date', 'last_modified_date', 'last_activity_date',
               'termination_date', 'timesheet_manager_resource_uid']
    cursor = mssql_connect.cursor()
    cursor.execute("""select
    [RES_UID] as resouce_uid,
    [RES_ID] as resource_id,
    [RES_TIMESHEET_MGR_UID] as timesheet_manager_resource_uid,
    [RES_NAME] as resource_name,
    [RES_GROUP] as department,
    [RES_TYPE] as resource_type,
    [RES_STD_RATE] as rate,
    [RES_OVT_RATE] as rate_overtime,
    [RES_MAX_UNITS] as max_units,
    [RES_IS_WINDOWS_USER] as is_person,
    [CREATED_DATE] as creation_date,
    [WRES_LAST_CONNECT_DATE] as last_activity_date,
    [MOD_DATE] as last_modified_date,
    [RES_HIRE_DATE] as hire_date,
    [RES_TERMINATION_DATE] as termination_date
from
    [PWA_Content].[pjpub].[MSP_RESOURCES]""")

    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    cursor.close()
    mssql_connect.close()



    truncate_data('resources')
    upload_data('resources', df)



def update_projects_stg():
    columns = ['uid','name','project_manager',
               'start_date','finish_date','created_date',
               'modified_date','actual_start_date','actual_finish_date',
               'percent_completed','percent_work_completed','arf_stage','class','status',
               'is_archived','work','actual_work','overtime_work']

    cursor = mssql_connect.cursor()
    cursor.execute("""SELECT
  [ProjectUID] as uid,
  p1.[ProjectName] as name,
  p1.[ProjectOwnerName] as project_manager,
  p1.[ProjectStartDate] as start_date,
  p1.[ProjectFinishDate] as finish_date,
  p1.[ProjectCreatedDate] as created_date,
  p1.[ProjectModifiedDate] as modified_date,
  p1.[ProjectActualStartDate] as actual_start_date,
  p1.[ProjectActualFinishDate] as actual_finish_date,
  p1.[ProjectPercentCompleted] as percent_completed,
  p1.[ProjectPercentWorkCompleted] as percent_work_completed,
  p1.[Этап АРФ] as arf_stage,
  p1.[Класс производственного проекта] as class,
  p1.[Статус производственного проекта] as status,
  p1.[Переведен в архив] as is_archived,
  p2.[ProjectWork] as work,
  p2.[ProjectActualWork] as actual_work,
  p2.[ProjectActualOvertimeWork] as overtime_work
FROM
  [PWA_CustomReports].[dbo].[Project_list_for_backup_select] p1 
  left join [PWA_CustomReports].[dbo].[PowerBI_PWA_ProjectsInfo] p2 on p1.ProjectName = p2.ProjectName""")

    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    cursor.close()
    mssql_connect.close()



    truncate_data('projects')
    upload_data('projects', df)



with DAG(
        dag_id='update_project_stg',
        schedule_interval='@daily',
        catchup=False,
        start_date=datetime(2022, 7, 1),
        tags=['update', 'project', 'sql', 'stg'],
        template_searchpath='/opt/airflow/dags/internal-bi-project'
) as dag:
    update_facts_stg = PythonOperator(task_id='update_facts_stg',
                                        python_callable=update_facts_stg)

    update_resources_stg = PythonOperator(task_id='update_resources_stg',
                                        python_callable=update_resources_stg)
    
    update_projects_stg = PythonOperator(task_id='update_projects_stg',
                                        python_callable=update_projects_stg)

update_facts_stg >> update_resources_stg >> update_projects_stg
