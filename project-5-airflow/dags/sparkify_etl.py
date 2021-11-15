from datetime import datetime, timedelta, date
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
DEBUG = True
S3_BUCKET = "udacity-dend"
EVENTS_KEY = "log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json"
if DEBUG:
    SONGS_KEY = "song_data/A/A/A/"
else:
    SONGS_KEY = "song_data/"

START_DATE = datetime(2018,11,1)

#end_date = datetime(2018,11,1,4,0,0)
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': START_DATE,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False    
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Use a Postgres operator to create tables in redshift
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
    s3_key=EVENTS_KEY
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

#
# TODO: resolve ID column
#
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

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    destination_table="public.users",
    destination_fields="""
        userid,
        first_name,
        last_name,
        gender,
        level
    """,
    source_select="""
        SELECT  se.userId,
                se.firstName,
                se.lastName,
                se.gender,
                se.level
            FROM staging_events se
            WHERE se.eventId = (
                SELECT  se0.eventId
                    FROM staging_events se0
                    WHERE se0.userId = se.userId
                    ORDER BY se0.ts DESC
                    LIMIT 1
            ) 
    """,
    redshift_conn_id="redshift",
    append=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    destination_table="public.songs",
    destination_fields="""
        songid,
        title,
        artistid,
        year,
        duration
    """,
    source_select="""
        SELECT  DISTINCT
                song_id,
                title,
                artist_id,
                year,
                duration
            FROM public.staging_songs
    """,
    redshift_conn_id="redshift",
    append=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    destination_table="public.artists",
    destination_fields="""
        artistid,
        name,
        location,
        lattitude,
        longitude
    """,
    source_select="""
        SELECT  DISTINCT
                artist_id,
                artist_name,
                artist_location,
                artist_latitude,
                artist_longitude
            FROM public.staging_songs ;
    """,
    redshift_conn_id="redshift",
    append=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    destination_table="public.time",
    destination_fields="""
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    """,
    source_select="""
        SELECT  DISTINCT
                start_time,
                EXTRACT(hour FROM start_time),
                EXTRACT(day FROM start_time),
                EXTRACT(week FROM start_time),
                EXTRACT(month FROM start_time),
                EXTRACT(year FROM start_time),
                EXTRACT(weekday FROM start_time)
            FROM public.songplays
    """,
    redshift_conn_id="redshift",
    append=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
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

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator