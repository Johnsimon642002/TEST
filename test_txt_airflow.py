from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
from datetime import datetime

def create_hello_file():
    folder_name = "airflow_hello_logs"
    os.makedirs(folder_name, exist_ok=True)  # Tạo thư mục nếu chưa có
    file_path = os.path.join(folder_name, "hello_airflow.txt")

    with open(file_path, "w", encoding="utf-8") as file:
        file.write("Hello, Airflow!\n")

# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 26),
    'retries': 1,
}

dag = DAG('hello_airflow_dag', default_args=default_args, schedule_interval='@once')

hello_task = PythonOperator(
    task_id='create_hello_file',
    python_callable=create_hello_file,
    dag=dag,
)

hello_task
