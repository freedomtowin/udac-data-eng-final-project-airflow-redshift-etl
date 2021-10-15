from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        {}
        """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="", # renders this value from context variables (reason: see line 8)
                 ftype="JSON",
                 ignore_headers=1,
                 region='us-west-2',
                 backfill_execution_date=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.ftype = ftype
        self.backfill_execution_date=backfill_execution_date

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type="s3")
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        execution_date = context["execution_date"]
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        if self.ftype=='JSON':
            formatting = "format as json 'auto' compupdate off"
        elif self.ftype=='PARQUET':
            formatting = "format as parquet compupdate off"
        elif self.ftype=='CSV':
            formatting="removequotes delimiter '|' emptyasnull blanksasnull maxerror 5"
        
        s3_key = self.s3_key
        
        if self.backfill_execution_date==True:
            s3_key = '/'.join([s3_key, str(execution_date.year), str(execution_date.month)]) 
        
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, s3_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            formatting
        )
        redshift.run(formatted_sql)
        self.log.info(f"Finished copying {self.table} from S3 to Redshift")



