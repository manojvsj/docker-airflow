from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

def print_hello():
    return 'Hello world!'

default_args = {
        'owner': 'shahina',
        'start_date':datetime(2018,8,11),
        'email': ['starnn.sapient@gmail.com', 'manojkumar.vsj@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': True,
}

dag = DAG('email_sample', 
          description='Simple tutorial DAG',
          schedule_interval='* * * * *',
          default_args = default_args, 
          catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

email = EmailOperator(
        task_id='send_email',
        to='starnn.sapient@gmail.com,manojkumar.vsj@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>Email Test</h3> """,
        dag=dag
)

email >> dummy_operator >> hello_operator
