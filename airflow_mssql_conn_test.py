import os
import pymssql
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_mssql"


def test_connection():
    conn = pymssql.connect(server='wacip-sandbox-airflowpoc.database.windows.net', user='svcairflowsqladmin',
                           password='cipAdmin1656$', database='airflow-audit-db-01')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM db_CIP_AUDIT.User_List;')
    print("connection is success")
    row = cursor.fetchone()
    while row:
        print(str(row[0]) + " " + str(row[1]) + " " + str(row[2]))
        row = cursor.fetchone()


with DAG(
        DAG_ID,
        schedule_interval='@daily',
        start_date=datetime(2021, 10, 1),
        tags=['example'],
        catchup=False,
) as dag:
    # Example of creating a task to select from database using PythonOperator pymssql
    test_conn_task = PythonOperator(task_id='test_conn_python', python_callable=test_connection)

    # Example of creating a task to create a table in MsSql using MsSqlOperator
    create_table_mssql_task = MsSqlOperator(
        task_id='create_country_table',
        mssql_conn_id='airflow_mssql',
        sql=r"""
        CREATE TABLE Country (
            country_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
            name TEXT,
            continent TEXT
        );
        """,
        database='airflow-audit-db-01',
        dag=dag,
    )


    @dag.task(task_id="insert_mssql_task")
    def insert_mssql_hook():
        mssql_hook = MsSqlHook(mssql_conn_id='airflow_mssql', schema='airflow-audit-db-01')

        rows = [
            ('India', 'Asia'),
            ('Germany', 'Europe'),
            ('Argentina', 'South America'),
            ('Ghana', 'Africa'),
            ('Japan', 'Asia'),
            ('Namibia', 'Africa'),
        ]
        target_fields = ['name', 'continent']
        mssql_hook.insert_rows(table='Country', rows=rows, target_fields=target_fields)


    # Example of creating a task that calls an sql command from an external file.
    create_table_mssql_from_external_file = MsSqlOperator(
        task_id='create_table_from_external_file',
        mssql_conn_id='airflow_mssql',
        sql='create_table.sql',
        database='airflow-audit-db-01',
        dag=dag,
    )
    populate_user_table = MsSqlOperator(
        task_id='populate_user_table',
        mssql_conn_id='airflow_mssql',
        sql=r"""
                INSERT INTO Users (username, description)
                VALUES ( 'Danny', 'Musician');
                INSERT INTO Users (username, description)
                VALUES ( 'Simone', 'Chef');
                INSERT INTO Users (username, description)
                VALUES ( 'Lily', 'Florist');
                INSERT INTO Users (username, description)
                VALUES ( 'Tim', 'Pet shop owner');
                """,
        database='airflow-audit-db-01',
    )
    get_all_countries = MsSqlOperator(
        task_id="get_all_countries",
        mssql_conn_id='airflow_mssql',
        sql=r"""SELECT * FROM Country;""",
        database='airflow-audit-db-01',
    )
    get_all_description = MsSqlOperator(
        task_id="get_all_description",
        mssql_conn_id='airflow_mssql',
        sql=r"""SELECT description FROM Users;""",
        database='airflow-audit-db-01',
    )
    get_countries_from_continent = MsSqlOperator(
        task_id="get_countries_from_continent",
        mssql_conn_id='airflow_mssql',
        sql=r"""SELECT * FROM Country where {{ params.column }}='{{ params.value }}';""",
        params={"column": "CONVERT(VARCHAR, continent)", "value": "Asia"},
        database='airflow-audit-db-01',
    )
    (
            test_conn_task
            >> create_table_mssql_task
            >> insert_mssql_hook()
            >> create_table_mssql_from_external_file
            >> populate_user_table
            >> get_all_countries
            >> get_all_description
            >> get_countries_from_continent
    )