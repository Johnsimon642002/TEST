from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys
import os
from datetime import datetime

def print_python_version():
    python_version = sys.version
    print(f"Python version: {python_version}")

    # Lưu vào file
    folder_name = "python_version_logs"
    os.makedirs(folder_name, exist_ok=True)  # Tạo thư mục nếu chưa tồn tại
    file_path = os.path.join(folder_name, "python_version.txt")

    # Ghi phiên bản Python vào file
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(f"Python version: {python_version}\n")

# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 26),
    'retries': 1,
}

dag = DAG('check_python_version_dag', default_args=default_args, schedule_interval='@once')

check_version_task = PythonOperator(
    task_id='check_python_version',
    python_callable=print_python_version,
    dag=dag,
)

check_version_task

