from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

current_directory = os.getcwd()
print("Current Working Directory:", current_directory)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    'simple_dag',
    default_args=default_args,
    description='A simple DAG with PythonOperator',
    schedule_interval=None,
)

def print_hello_task(task_number):
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"Hello task {task_number} - {current_time}"
    print(message)
    with open(f"/opt/airflow/hello_task.txt", "a") as file:
        file.write(message + "\n")
        if task_number==10:
            file.write(f"{current_directory} \n\n")

tasks = []

for i in range(1, 11):
    task = PythonOperator(
        task_id=f'hello_task_{i}',
        python_callable=print_hello_task,
        op_kwargs={'task_number': i},
        dag=dag
    )

    tasks.append(task)

tasks[0] >> tasks[1] >> tasks[2] >> tasks[3] >> tasks[4] >> tasks[5] >> tasks[6] >> tasks[7] >> tasks[8] >> tasks[9]