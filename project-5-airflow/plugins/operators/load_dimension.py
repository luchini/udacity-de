from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
        
class LoadDimensionOperator(BaseOperator):
    """Airflow operator for loading dimension tables

    """
    insert_sql_template = """
    INSERT INTO {destination_table} (
        {destination_fields}
    )
    {source_select}
    """
    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self,
                 destination_table="",
                 destination_fields="",
                 source_select="",
                 redshift_conn_id="",
                 append=True,
                 *args, **kwargs):
        """Init for LoadDimensionOperator

        Parameters:
        - destination_table
        - destination_fields
        - source_select
        - redshift_conn_id
        - append
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        #
        # Map params
        #
        self.destination_table = destination_table
        self.insert_sql = self.insert_sql_template.format(**locals())
        self.redshift_conn_id = redshift_conn_id
        self.append=append
        
    def execute(self, context):
        """Copy to the dimension table. Optionally, wipe the table first
        
        Parameters:
        - self
        - context
        """

        #
        # Open a redshift connection with PostgresHook
        #
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #
        # Delete existing data
        #
        if not self.append:
            self.log.info(f"Clearing data from Redshift dimension table {self.destination_table}")
            redshift.run(f"DELETE FROM {self.destination_table}")
        else:
            self.log.info(f"Preparing to append data to Redshift dimension table {self.destination_table}")
        
        #
        # Run insert statement
        #
        self.log.info(f"Inserting data to Redshift dimension table {self.destination_table}")
        redshift.run(self.insert_sql)