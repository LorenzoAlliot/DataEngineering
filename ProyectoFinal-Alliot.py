from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from common.Conn import create_db_connection_string,create_tables
from common.ETL import load_data
from common.email import enviar_ok,enviar_no  
import sys

sys.path.append('/opt/airflow')

# Crea el objeto DAG
dag = DAG(
    dag_id='proyectofinal_alliot',
    description='Un DAG para obtener y cargar datos',
    start_date=datetime(2023, 9, 27),
    on_success_callback=enviar_ok,
    on_failure_callback=enviar_no,
    schedule_interval='@daily',  # Define la frecuencia de ejecución
)

# Define tareas como operadores Python


db_conn_task = PythonOperator(
    task_id='create_db_connection',
    python_callable=create_db_connection_string,  # Llama a la función de cadena de conexión
    provide_context=True,  # Esto permite que Airflow pase el contexto a la función
    retries= 3,
    retry_delay= timedelta(minutes=2),
    on_success_callback=enviar_ok,
    on_failure_callback=enviar_no,
    dag=dag,
)

ETL_task = PythonOperator(
    task_id='ETL',
    python_callable=load_data,
    op_args=create_db_connection_string(),  # Pasa la cadena de conexión
    retries= 3,
    on_success_callback=enviar_ok,
    on_failure_callback=enviar_no,
    retry_delay= timedelta(minutes=5),
    dag=dag,
)

# Define el orden de ejecución de las tareas
db_conn_task >> ETL_task
