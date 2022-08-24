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

    update_resources_mapping = PostgresOperator(
        task_id='update_resources_mapping',
        postgres_conn_id='bi',
        sql="""call project.resources_mapping_insert()""",
        autocommit=True
    )

    update_resources = PostgresOperator(
        task_id='update_resources',
        postgres_conn_id='bi',
        sql="""call project.resources_insert()""",
        autocommit=True
    )

    update_rates = PostgresOperator(
        task_id='update_rates',
        postgres_conn_id='bi',
        sql="""call project.rates_insert()""",
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

    update_timesheet_status = PostgresOperator(
        task_id='update_timesheet_status',
        postgres_conn_id='bi',
        sql="""call project.timesheet_status_insert()""",
        autocommit=True

    )

    update_timesheet_line_status = PostgresOperator(
        task_id='update_timesheet_line_status',
        postgres_conn_id='bi',
        sql="""call project.timesheet_line_status_insert()""",
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

update_departments >> update_resources_mapping \
>> update_resources >> update_rates >> update_projects \
>> update_tasks >> update_assignments \
>> update_timesheet_status >> update_timesheet_line_status \
>> update_timesheet_lines >> update_facts
