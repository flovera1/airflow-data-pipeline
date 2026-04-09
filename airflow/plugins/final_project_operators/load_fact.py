from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

class LoadFactOperator(BaseOperator):

    @apply_defaults
    def __init__(self, redshift_conn_id="", table="", sql="", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Inserting into fact table: {self.table}")

        try:
            redshift.run(f"INSERT INTO {self.table} {self.sql}")
            self.log.info(f"Finished loading fact table: {self.table}")
        except Exception as e:
            self.log.error(f"Error loading fact table {self.table}: {e}")
            raise
