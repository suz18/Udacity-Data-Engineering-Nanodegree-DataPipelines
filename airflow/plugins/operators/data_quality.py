from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table} WHERE songid is NULL")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check passed. {self.table} contained 0 rows of null songid")
        logging.info(f"Data quality on table {self.table} check failed with {records[0][0]} null records in the table")