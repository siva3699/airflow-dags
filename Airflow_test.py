from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022,8,4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
   
}

dag = DAG("helloworld_test", default_args=default_args, schedule_interval=timedelta(1),catchup=False)
t1 = BashOperator(
task_id='testairflow',
bash_command='echo "hello"',
dag=dag)
t1
