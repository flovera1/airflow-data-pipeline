from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self, redshift_conn_id="", tests=[], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test in self.tests:
            result = redshift.get_first(test["check_sql"])[0]

            if test.get("comparison") == "greater_than":
                if result <= test["expected_result"]:
                    raise ValueError("Data quality check failed")
            else:
                if result != test["expected_result"]:
                    raise ValueError("Data quality check failed")
