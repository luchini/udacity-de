from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """Airflow operator for copying from s3 to Redshift
    """
    template_fields = ("s3_key",)
    ui_color = '#358140'
    copy_sql = """
        COPY {}.{}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
    """
    
    @apply_defaults
    def __init__(self,
                schema="public",
                 table="",
                 redshift_conn_id="",
                 aws_credentials_id="", 
                 s3_bucket="",
                 s3_key="",
                 json="auto",
                 *args, **kwargs):
        """Init the staging operator
        
        Parameters
        - table
        - redshift_conn_id
        - aws_credentials-id
        - s3_bucket
        - s3_key
        - json
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        #
        # Map params
        #
        self.schema = schema
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json

    def execute(self, context):
        """Clear from the destination table, then copy from S3 into Redshift

        Parameters:
        - self
        - context
        """
        #
        # Use AwsHook to get credentials
        #
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        #
        # Open a redshift connection with PostgresHook
        #
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #
        # Delete existing data
        #
        self.log.info(f"Clearing data from destination Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.schema}.{self.table}")
        
        #
        # Render the s3 key using context variables
        #
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        
        #
        # Format copy statement
        #
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.schema,
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json
        )
        
        #
        # Copy the data
        #
        self.log.info(f"Copying data to destination Redshift table {self.table}")
        redshift.run(formatted_sql)