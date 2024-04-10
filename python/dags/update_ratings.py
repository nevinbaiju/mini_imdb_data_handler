from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from tasks.update_rating_tasks import *

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    'update_ratings',
    default_args=default_args,
    max_active_runs=1,
    description='Updates the ratings from rating dumps',
    schedule_interval=timedelta(minutes=30),
)

t_spark_aggregate = PythonOperator(
                                    task_id='spark_aggregate',
                                    python_callable=spark_aggregate,
                                    provide_context=True,
                                    dag=dag
                                )
t_delete_files = PythonOperator(
                                    task_id='delete_files',
                                    python_callable=delete_files,
                                    provide_context=True,
                                    dag=dag,
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

t_spark_aggregate >> [t_update_ranks, t_delete_files] >> t_find_top_10