from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd 

def first_funtion_execute(**context):
   print("first function execution")
   context['ti'].xcom_push(key="mykey",value="first function execution say hello")

def second_funtion_execute(**context):
    instance=context.get('ti').xcom_pull(key="mykey")
    print("i am in second function execution and got value {} from first function".format(instance))


with DAG(
    dag_id='first_dag',
    schedule_interval="@daily",
    default_args={
        "owner":"airflow",
        "retries":1,
        "retry_delay":timedelta(minutes=5),
        "start_date":datetime(2024,4,3)
    },
    catchup=False
) as f:
    first_funtion_execute=PythonOperator(
        task_id="first_function_execute",
        python_callable=first_funtion_execute,
        provide_context=True
    )
    second_funtion_execute=PythonOperator(
        task_id="second_function_execute",
        python_callable=second_funtion_execute,
        provide_context=True
    )
    
first_funtion_execute>>second_funtion_execute
