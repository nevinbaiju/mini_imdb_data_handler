from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from tasks.update_rating_tasks import *

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    'update_ratings',
    default_args=default_args,
    description='Updates the ratings from rating dumps',
    schedule_interval=None,
)

t_spark_aggregate = PythonOperator(
                                    task_id='spark_aggregate',
                                    python_callable=spark_aggregate,
                                    dag=dag
                                )
t_update_ranks = PythonOperator(
                                    task_id='update_ranks',
                                    python_callable=update_ranks,
                                    dag=dag
                                )
t_find_top_10 = PythonOperator(
                                    task_id='find_top_10',
                                    python_callable=find_top_10,
                                    dag=dag
                                )                                

t_spark_aggregate >> t_update_ranks >> t_find_top_10