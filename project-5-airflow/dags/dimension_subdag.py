from airflow import DAG
from airflow.operators import LoadDimensionOperator

from helpers import SqlQueries

#
# Returns a DAG which loads all dimension tables. We encapsulated the dimension 
# load into a subdag to simplify the dag.
#
def load_dimension_tables_subdag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        *args, **kwargs):
    """Create and return the subdag for loading dimension tables
    Parameters:
        - parent_dag_name
        - task_id
        - redshift_conn_id
    """

    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        destination_table="public.users",
        source_select=SqlQueries.user_table_insert,
        redshift_conn_id=redshift_conn_id,
        append=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        destination_table="public.songs",
        source_select=SqlQueries.song_table_insert,
        redshift_conn_id=redshift_conn_id,
        append=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        destination_table="public.artists",
        source_select=SqlQueries.artist_table_insert,
        redshift_conn_id=redshift_conn_id,
        append=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        destination_table="public.time",
        source_select=SqlQueries.time_table_insert,
        redshift_conn_id=redshift_conn_id,
        append=False
    )
    
    return dag

