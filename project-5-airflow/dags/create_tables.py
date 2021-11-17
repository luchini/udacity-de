from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

"""Simple, on-off dag to drop/recreate tables in redshift
"""

#
# Set variables, backdate start date
#
START_DATE = datetime(2020,1,1)

#
# Construct default arguments
#
default_args = {
    'owner': 'udacity',
    'start_date': START_DATE,
    'email_on_retry': False,
    'depends_on_past': False
}

dag = DAG('create_sparkify_tables_dag',
          default_args=default_args,
          description='One-off to drop/recreate sparkify tables in redshift',
          schedule_interval=None,
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#
# Use a Postgres operator to create tables in redshift in debug mode
#
create_tables_in_redshift = PostgresOperator(
    task_id="Create_tables",
    dag=dag,
    sql="create_tables.sql",
    postgres_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Configure dependencies
#
start_operator >> create_tables_in_redshift
create_tables_in_redshift >> end_operator