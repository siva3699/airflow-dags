import os
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator as snop

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_SCHEMA = 'SRC_STG_TEST'
SNOWFLAKE_WAREHOUSE = 'CIPEDWDEVELOPERSWH'
SNOWFLAKE_DATABASE = 'EDW_DEV'
SNOWFLAKE_ROLE = 'CIP_EDW_SB_DEV'
v_STORED_PROC_NAME = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.SP_LOAD_SRCSTG_Patron()"
DAG_ID = "dag_load_src_stg_tables"
with DAG(
        DAG_ID,
        start_date=datetime(2022, 1, 1),
        default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags=['load_src_stg'],
        catchup=False,
) as dag:
    sp_load_patron = SnowflakeOperator(
        task_id='sp_load_patron',
        sql=f"call {v_STORED_PROC_NAME}",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )
    
sp_load_patron