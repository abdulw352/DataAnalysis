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
    dag_id = "ETL_Server_Access_Log_Processing",
    default_args = default_args,
    description = "Sample ETL DAG using BASH",
    schedule_interval = timedelta(days=1),
)

# defining the tasks 


# defining task "download"
download = BashOperator(
    task_id = 'doanload',
    bash_command = 'wget  https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt',
    dag=dag
)

# defining the first task named: extract 
extract = BashOperator(
    task_id = 'extract',
    bash_command = 'cut -f1,4 -d"#" web-server-access-log.txt > /home/project/airflow/dags/extracted.txt',
    dag=dag,
)

# defining the second task named: transform
transform = BashOperator(
    task_id = 'transform',
    bash_command = 'tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/extracted.txt > home/project/airflow/dags/capitalized.txt ',
    dag = dag
)

# defining the third tasks named: load
load = BashOperator(
    task_id = 'load',
    bash_command = 'zip log.zip capitalized.txt',
    dag = dag
)

# task pipeline
download >> extract >> transform >> load


