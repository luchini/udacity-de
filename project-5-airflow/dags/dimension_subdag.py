from airflow import DAG
from airflow.operators import LoadDimensionOperator

#
# Returns a DAG which loads all dimension tables. Since the SQL select statments are quite verbose,
# we encapsulated the dimension load into a subdag to simplify the dag.
#
def load_dimension_tables_subdag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
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
                    SELECT  se0.eventid
                        FROM staging_events se0
                        WHERE se0.userId = se.userId
                        ORDER BY se0.ts DESC
                        LIMIT 1
                ) 
        """,
        redshift_conn_id=redshift_conn_id,
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
        redshift_conn_id=redshift_conn_id,
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
        redshift_conn_id=redshift_conn_id,
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
        redshift_conn_id=redshift_conn_id,
        append=False
    )
    
    return dag

