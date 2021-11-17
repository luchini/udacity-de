from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """ Airflow operator for loading a fact table
    """
    insert_sql_template = """
    INSERT INTO {destination_table} 
    {source_select}
    """
    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self,
                 destination_table="",
                 source_select="",
                 redshift_conn_id="",
                 *args, **kwargs):

        """ Instantiate the LoadFactOperator

        Parameters:
        - destination_table
        - source_select
        - redshift_conn_id
        - append
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        #
        # Map params
        #
        self.destination_table = destination_table
        self.insert_sql = self.insert_sql_template.format(**locals())
        self.redshift_conn_id = redshift_conn_id
        
    def execute(self, context):
        """Copy to the fact table
        
        Parameters:
        - self
        - context
        """
        #
        # Open a redshift connection with PostgresHook
        #
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
                
        #
        # Run insert statement
        #
        self.log.info(f"Inserting data to Redshift fact table {self.destination_table}")
        redshift.run(self.insert_sql)