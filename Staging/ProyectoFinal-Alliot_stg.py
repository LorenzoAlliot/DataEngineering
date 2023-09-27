from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from common.Conn_stg import create_db_connection_string,create_tables
from common.ETL import load_data_stg, load_data_final
import sys


sys.path.append('/opt/airflow')

# Crea el objeto DAG
dag = DAG(
    dag_id='proyectofinal_alliot',
    description='Un DAG para obtener y cargar datos',
    start_date=datetime(2023, 9, 26),
    schedule_interval='@daily',  # Define la frecuencia de ejecución
)

# Define tareas como operadores Python


db_conn_task = PythonOperator(
    task_id='create_db_connection',
    python_callable=create_db_connection_string,  # Llama a la función de cadena de conexión
    provide_context=True,  # Esto permite que Airflow pase el contexto a la función
    dag=dag,
)


create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables,  # Llama a la función de cadena de conexión
    op_args=create_db_connection_string(),  # Esto permite que Airflow pase el contexto a la función
    dag=dag,
)

load_data_stg_task = PythonOperator(
    task_id='load_data_stg',
    python_callable=load_data_stg,
    op_args=create_db_connection_string(),  # Pasa la cadena de conexión
    dag=dag,
)



load_final_data_task = PythonOperator(
    task_id='load_data_final',
    python_callable=load_data_final,  # Llama a la función copy_data
    op_args=create_db_connection_string(),  # Esto permite que Airflow pase el contexto a la función
    dag=dag,
)

# Define el orden de ejecución de las tareas
db_conn_task >> create_tables_task >> load_data_stg_task >> load_final_data_task
