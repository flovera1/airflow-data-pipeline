from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

class StageToRedshiftOperator(BaseOperator):

    @apply_defaults
    def __init__(self, redshift_conn_id="", aws_credentials_id="", table="", s3_bucket="", s3_key="", json_path="auto", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Starting load for table: {self.table}")

        try:
            redshift.run(f"DELETE FROM {self.table}")
            self.log.info(f"Cleared table: {self.table}")

            s3_path = f"s3://{self.s3_bucket}/{self.s3_key.format(**context)}"

            copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            FORMAT AS JSON '{self.json_path}'
            REGION 'us-west-2';
            """

            self.log.info(f"Copying data from {s3_path}")
            redshift.run(copy_sql)

            self.log.info(f"Finished loading {self.table}")

        except Exception as e:
            self.log.error(f"Error loading {self.table}: {e}")
            raise