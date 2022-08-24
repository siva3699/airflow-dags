from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
default_args = {
        'owner': 'airflow',
        'start_date': datetime(2022,8,16)
        
}

with DAG('parallel_dag',default_args=default_args,description='First DAG', schedule_interval= '@daily', default_args=default_args,catchup=False)as dag:
        task_1=BashOperator(
            task_id='task_1',
            bash_command='sleep 3'
            
        )
        
        
        task_2=BashOperator(
            task_id='task_2',
            bash_command='sleep 3'
            
        )
        
        
        task_3=BashOperator(
            task_id='task_3',
            bash_command='sleep 3'  

        )

        task_4=BashOperator(
            task_id='task_4',
            bash_command='sleep 3'
            
            
        )
        
        
        task_1 >> [task_2,task_3] >> task_4