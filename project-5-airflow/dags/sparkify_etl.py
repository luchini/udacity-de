from datetime import datetime, timedelta, date
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.subdag_operator import SubDagOperator
from dimension_subdag import load_dimension_tables_subdag

from helpers import SqlQueries

DEBUG = False
S3_BUCKET = "udacity-dend"
EVENTS_KEY = "log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json"
if DEBUG:
    SONGS_KEY = "song_data/A/A/A/"
else:
    SONGS_KEY = "song_data/"

START_DATE = datetime(2018,11,1)
END_DATE = None
#END_DATE = datetime(2018,11,1,4,0,0)

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': START_DATE,
    'end_date' : END_DATE,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False    
}

dag = DAG('sparkify_etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily', #'@hourly',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#
# Use a Postgres operator to create tables in redshift
#
create_tables_in_redshift = PostgresOperator(
    task_id="Create_tables",
    dag=dag,
    sql="create_tables.sql",
    postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="public.staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=S3_BUCKET,
    s3_key=EVENTS_KEY,
    json=f"s3://{S3_BUCKET}/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="public.staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=S3_BUCKET,
    s3_key=SONGS_KEY
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    destination_table="public.songplays",
    destination_fields="""
        start_time,
        userid,
        level,
        songid,
        artistid,
        sessionid,
        location,
        user_agent
    """,
    source_select="""
        SELECT  DISTINCT
                (TIMESTAMP 'epoch' + se.ts/1000 * interval '1 Second'),
                se.userId,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionId,
                se.location,
                se.userAgent
            FROM public.staging_events se
            INNER JOIN public.staging_songs ss
                ON ss.artist_name = se.artist
                AND ss.title = se.song
                AND ss.duration = se.length
            WHERE se.page = 'NextSong'
    """,
    redshift_conn_id="redshift"
)

#
# Create subdag task for loading dimension tables
#
load_dim_task_id = "load_dimension_subdag"
dim_subdag_task = SubDagOperator(
    subdag=load_dimension_tables_subdag(
        parent_dag_name="sparkify_etl_dag",
        task_id=load_dim_task_id,
        redshift_conn_id="redshift",
        start_date=START_DATE
    ),
    task_id=load_dim_task_id,
    dag=dag
)

#
# Run some quality checks on our data
#
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    sql_checks = [
        {   "info"  : "time", 
            "sql"   : "SELECT COUNT(*) FROM public.time WHERE start_time IS NULL",
            "value" : 0},
        {   "info"  : "artists", 
            "sql"   : "SELECT COUNT(*) FROM public.artists WHERE artistid IS NULL",
            "value" : 0},
        {   "info"  : "songs",
            "sql"   : "SELECT COUNT(*) FROM public.songs WHERE artistid IS NULL",
            "value" : 0}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Configure dependencies
#
start_operator >> create_tables_in_redshift

create_tables_in_redshift >> stage_events_to_redshift
create_tables_in_redshift >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> dim_subdag_task
dim_subdag_task >> run_quality_checks

run_quality_checks >> end_operator