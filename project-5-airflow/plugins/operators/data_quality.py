from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        #
        # Map params
        #
        self.redshift_conn_id = redshift_conn_id
        self.sql_checks = sql_checks

    def execute(self, context):
        #
        # Open a redshift connection with PostgresHook
        #
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for check in self.sql_checks:
            info = check["info"]
            sql = check["sql"]
            value = check["value"]

            #
            # Execute sql expression
            #
            records = redshift.get_records(sql)

            #
            # Check that it executed successfully
            #
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {info} query returned no results")
            
            #
            # Check the results match
            #
            num_records = records[0][0]
            if num_records != value:
                raise ValueError(f"Data quality check failed. {info} query returned {num_records} results")

            self.log.info(f"Data quality check for {info} passed.")