from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
        dag_id='update_project',
        schedule_interval='@hourly',
        catchup=False,
        start_date=datetime(2022, 7, 1),
        tags=['update', 'project', 'sql'],
        template_searchpath='/opt/airflow/dags/internal-bi-project'
) as dag:
    update_departments = PostgresOperator(
        task_id='update_departments',
        postgres_conn_id='bi',
        sql="""call project.departments_insert()""",
        autocommit=True
    )

    update_resources = PostgresOperator(
        task_id='update_resources',
        postgres_conn_id='bi',
        sql="""call project.resources_insert()""",
        autocommit=True
    )

    update_projects = PostgresOperator(
        task_id='update_projects',
        postgres_conn_id='bi',
        sql="""call project.projects_insert()""",
        autocommit=True
    )

    update_tasks = PostgresOperator(
        task_id='update_tasks',
        postgres_conn_id='bi',
        sql="""call project.tasks_insert()""",
        autocommit=True
    )

    update_assignments = PostgresOperator(
        task_id='update_assignments',
        postgres_conn_id='bi',
        sql="""call project.assignments_insert()""",
        autocommit=True
    )

    update_timesheet_statuses = PostgresOperator(
        task_id='update_timesheet_statuses',
        postgres_conn_id='bi',
        sql="""call project.timesheet_statuses_insert()""",
        autocommit=True

    )

    update_timesheet_line_statuses = PostgresOperator(
        task_id='update_timesheet_line_statuses',
        postgres_conn_id='bi',
        sql="""call project.timesheet_line_statuses_insert()""",
        autocommit=True
    )

    update_timesheet_lines = PostgresOperator(
        task_id='update_timesheet_lines',
        postgres_conn_id='bi',
        sql="""call project.timesheet_lines_insert()""",
        autocommit=True
    )

    update_facts = PostgresOperator(
        task_id='update_facts',
        postgres_conn_id='bi',
        sql="""call project.facts_insert()""",
        autocommit=True
    )

update_departments  \
>> update_resources  >> update_projects \
>> update_tasks >> update_assignments \
>> update_timesheet_statuses >> update_timesheet_line_statuses \
>> update_timesheet_lines >> update_facts
