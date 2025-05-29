from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

class PostgreSQLCountRows(BaseOperator):
    template_fields = ("table_name",)

    def __init__(self, table_name: str, conn_id: str = "postgres_default", **kwargs):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.postgres_conn_id = conn_id

    def execute(self, context: Context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        sql = f"SELECT COUNT(*) FROM {self.table_name};"
        self.log.info(f"Executing SQL: {sql}")
        count = hook.get_first(sql)[0]
        self.log.info(f"Row count for table {self.table_name}: {count}")
        return count