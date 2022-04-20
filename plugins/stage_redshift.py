from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 aws_key,
                 aws_secret,
                 redshift_conn_id,
                 s3_bucket,
                 s3_key,
                 table,
                 json_conf,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.json_conf = json_conf

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Copying data from S3 to Redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket, rendered_key)
        self.log.info('S3 Path {}'.format(s3_path))

        formatted_sql = ("""
                copy "dev"."public"."{}" from '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                json '{}'
                region 'us-west-2';
                """).format(
            self.table,
            s3_path,
            self.aws_key,
            self.aws_secret,
            self.json_conf
        )
        self.log.info('Run copying query')
        self.log.info(formatted_sql)
        redshift.run(formatted_sql)
        self.log.info('END')





