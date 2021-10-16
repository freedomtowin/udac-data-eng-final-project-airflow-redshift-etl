from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class RowCountOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(RowCountOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f"Data quality check failed. {table} returned no results")
                raise ValueError(f"Data quality check failed. {table} returned no results")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
            
            

class NullPercentOperator(BaseOperator):

    ui_color = '#89DA59'
    copy_sql = """
                select
                   column_name
                from information_schema.columns
                where table_name = '{TABLE}'
                order by ordinal_position;
                """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(NullPercentOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook_1 = PostgresHook(self.redshift_conn_id)
        redshift_hook_2 = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            columns = redshift_hook_1.get_records(NullPercentOperator.copy_sql.format(**{"TABLE":table}))
            for col in columns:
                col = col[0]
                records = redshift_hook_2.get_records(f"SELECT SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END)/COUNT(*) FROM {table}")
                if len(records) < 1 or len(records[0]) < 1 or records[0][0] > 0.9:
                    self.log.error(f"Data quality check failed. {table} {col} has >90% nulls,{records[0][0]}")
                    raise ValueError(f"Data quality check failed. {table} {col} has >90% nulls, {records[0][0]}")
                self.log.info(f"Data quality on table {table} {col} check passed with {records[0][0]} %null values")