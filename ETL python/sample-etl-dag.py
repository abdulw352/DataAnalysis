# import the libraries 

from datetime import timedelta
# The DAG object; to instanstiate a DAG
from airflow import DAG 
# Operators; for writing tasks 
from airflow.operators.bash_operator import BashOperator
# for easy scheduling easy
from airflow.utils.dates import days_ago

# Defining DAG arguments 

default_args = {
    "owner" : "Name",
    "start_date" : days_ago(0),
    "email" : ["test@test.com"],
    "email_on_failure" : False,
    "email_on_retry" : False,
    "retries" : 1,
    "retry_delay" : timedelta(minutes=5) 
}

# Define the DAG
dag = DAG(
    dag_id = "sample-etl-dag",
    default_args = default_args,
    description = "Sample ETL DAG using BASH",
    schedule_interval = timedelta(days=1),
)

# defining the tasks 

# defining the first task named: extract 
extract = BashOperator(
    task_id = 'extract',
    bash_command = 'cut -d":" -f1,3,6 /etc/passwd > /home/project/airflow/dags/extracted-data.txt',
    dag=dag,
)

# defining the second task named: transform
transform_and_load = BashOperator(
    task_id = 'transform',
    bash_command = 'tr ":" "," < /home/project/airflow/dags/extracted-data.txt > home/project/airflow/dags/transformed-data.csv ',
    dag = dag
)


# task pipeline
extract >> transform_and_load


