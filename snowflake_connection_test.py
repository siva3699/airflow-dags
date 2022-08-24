#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example use of Snowflake related operators.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator

# from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
# SLACK_CONN_ID = 'my_slack_conn'
# TODO: should be able to rely on connection's schema, but currently param required by S3ToSnowflakeTransfer
SNOWFLAKE_SCHEMA = 'AUDIT_TEST'
# SNOWFLAKE_STAGE = 'stage_name'
SNOWFLAKE_WAREHOUSE = 'CIPEDWDEVELOPERSWH'
SNOWFLAKE_DATABASE = 'EDW_DEV'
SNOWFLAKE_ROLE = 'CIP_EDW_SB_DEV'
SNOWFLAKE_SAMPLE_TABLE = 'AIRFLOWCONNECTIONTEST'

# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TABLE {SNOWFLAKE_SAMPLE_TABLE} (name VARCHAR(250), id INT);"
)
SQL_INSERT_STATEMENT = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} VALUES ('Test', 1)"
SQL_INSERT_STATEMENT1 = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} VALUES ('Test1', 12)"
#SQL_CALL_SP = f"CALL SP_LOAD_SRCSTG_PATRON();"

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_snowflake_new"

# [START howto_operator_snowflake]

with DAG(
        DAG_ID,
        start_date=datetime(2021, 1, 1),
        default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags=['example'],
        catchup=False,
) as dag:
    # [START snowflake_example_dag]
    snowflake_op_sql_ddl = SnowflakeOperator(
        task_id='snowflake_op_sql_ddl',
        sql=CREATE_TABLE_SQL_STRING,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_op_sql_dmls = SnowflakeOperator(
        task_id='snowflake_op_sql_dmls',
        sql=SQL_INSERT_STATEMENT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

# [END snowflake_example_dag]
(
        snowflake_op_sql_ddl
        >> snowflake_op_sql_dmls
)
