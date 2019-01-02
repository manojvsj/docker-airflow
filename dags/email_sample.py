
"""
Do proper smtp configuration in airflow.cfg file

[smtp]
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_user = abc@gmail.com
smtp_port = 587
smtp_password = password@1234567
smtp_mail_from = abc@gmail.com
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

def print_hello():
    print("Hello")

default_args = {
        'owner': 'airflow',
        'start_date':datetime(2018,8,11),
        'email': ['abc@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': True,
}

dag = DAG('email_sample',
          description='Simple email task',
          schedule_interval='*/5 * * * *',
          default_args = default_args,
          catchup=False)

dummy_operator = DummyOperator(
    task_id='dummy_task',
    retries=3,
    dag=dag)

hello_operator = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag)

email = EmailOperator(
        task_id='send_email',
        to='abcdef@gmail.com,abc@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>Email Test</h3> """,
        dag=dag
)

email >> dummy_operator >> hello_operator
