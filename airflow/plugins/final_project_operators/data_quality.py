from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self, redshift_conn_id="", tests=None, *args, **kwargs):
        super().__init__(*args, **kwargs)  # ADD THIS
        self.redshift_conn_id = redshift_conn_id  # ADD THIS
        self.tests = tests or []

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test in self.tests:
            sql = test["check_sql"]
            expected = test["expected_result"]
            comparison = test.get("comparison", "equal")

            result = redshift.get_first(sql)[0]

            self.log.info(f"Running check: {sql} | Result: {result}")

            if comparison == "greater_than" and result <= expected:
                raise ValueError(f"Check failed: {sql}")

            if comparison == "equal" and result != expected:
                raise ValueError(f"Check failed: {sql}")

            if comparison not in ["greater_than", "equal"]:
                raise ValueError(f"Invalid comparison type: {comparison}")