from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

class LoadDimensionOperator(BaseOperator):

    @apply_defaults
    def __init__(self, redshift_conn_id="", table="", sql="", mode="append", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.mode == "truncate-insert":
            redshift.run(f"TRUNCATE TABLE {self.table}")

        redshift.run(f"INSERT INTO {self.table} {self.sql}")
