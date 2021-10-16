from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, RowCountOperator, NullPercentOperator, PostGresOperator)
from helpers import SqlQueries
#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'rohan',
    'start_date': datetime(2020, 1, 1),
    'end_date': datetime(2020, 2, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'catchup': True,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_blocks_to_redshift = StageToRedshiftOperator(
    task_id='Stage_blocks',
    dag=dag,
    table='staging_blocks',
    redshift_conn_id="redshift_id",
    aws_credentials_id="aws_credentials",
    s3_bucket="rk-blockchain-db",
    s3_key="block",
    ftype='CSV',
    region='us-east-1',
    backfill_execution_date=True
)

stage_transaction_to_redshift = StageToRedshiftOperator(
    task_id='Stage_transactions',
    dag=dag,
    table='staging_transactions',
    redshift_conn_id="redshift_id",
    aws_credentials_id="aws_credentials",
    s3_bucket="rk-blockchain-db",
    s3_key="transaction",
    ftype='CSV',
    region='us-east-1',
    backfill_execution_date=True
)

stage_prices_to_redshift = StageToRedshiftOperator(
    task_id='Stage_prices',
    dag=dag,
    table='staging_prices',
    redshift_conn_id="redshift_id",
    aws_credentials_id="aws_credentials",
    s3_bucket="rk-blockchain-db",
    s3_key="price",
    ftype='JSON',
    ignore_headers=0,
    region='us-east-1',
    backfill_execution_date=True
)


load_block_transaction_table = LoadFactOperator(
    task_id='Load_block_transaction_fact_table',
    dag=dag,
    redshift_conn_id='redshift_id',
    sql_query=SqlQueries.block_transaction_table_insert
)

load_block_dimension_table = LoadDimensionOperator(
    task_id='Load_block_dim_table',
    dag=dag,
    redshift_conn_id='redshift_id',
    table="block",
    sql_query=SqlQueries.block_table_insert
)

load_transaction_dimension_table = LoadDimensionOperator(
    task_id='Load_transaction_dim_table',
    dag=dag,
    redshift_conn_id='redshift_id',
    table="transactions",
    sql_query=SqlQueries.transaction_table_insert
)

load_price_dimension_table = LoadDimensionOperator(
    task_id='Load_price_dim_table',
    dag=dag,
    redshift_conn_id='redshift_id',
    table="price",
    sql_query=SqlQueries.price_table_insert
)

run_quality_checks_row_count = RowCountOperator(
    task_id='Run_data_quality_checks_row_cnt',
    dag=dag,
    redshift_conn_id='redshift_id',
    tables=["transactions", "block", "block_transaction", "price"]
)

run_quality_checks_null_per = NullPercentOperator(
    task_id='Run_data_quality_checks_null_per',
    dag=dag,
    redshift_conn_id='redshift_id',
    tables=[ "block_transaction"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator>> stage_blocks_to_redshift
start_operator>> stage_transaction_to_redshift
start_operator>> stage_prices_to_redshift

[stage_blocks_to_redshift, stage_transaction_to_redshift, stage_prices_to_redshift] >> load_block_transaction_table

load_block_transaction_table >> load_transaction_dimension_table
load_block_transaction_table >> load_block_dimension_table
load_block_transaction_table >> load_price_dimension_table

load_transaction_dimension_table >> run_quality_checks_row_count
load_block_dimension_table >> run_quality_checks_row_count
load_price_dimension_table >> run_quality_checks_row_count

run_quality_checks_row_count >> run_quality_checks_null_per >> end_operator






