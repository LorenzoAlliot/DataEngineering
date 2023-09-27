import pandas as pd
import sqlalchemy as sa
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime , date, time
import os
from Config.config import read_config
from common.ETL import extract_and_process_data

# Función para crear una conexión a la base de datos
def create_db_connection_string():
    conn_data = read_config()["Redshift"]
    host = conn_data["host"]
    port = conn_data["port"]
    db = conn_data["db"]
    user = conn_data["user"]
    pwd = conn_data["pwd"]
    dbschema = f'{user}'

    conn_string = f"postgresql://{user}:{pwd}@{host}:5439/{db}"
    return conn_string, dbschema



# Función para crear tablas en la base de datos
def create_tables_in_db(conn):
    conn.execute("""
CREATE TABLE IF NOT EXISTS lorenzoalliot_coderhouse.leagues(
	idLeague INTEGER NOT NULL,
	name VARCHAR(255),
	codeCountry VARCHAR(3),
	logo VARCHAR(1000),
	season INTEGER,
	type VARCHAR(100),
	startLeague VARCHAR(20),
	endLeague VARCHAR(20),

	PRIMARY KEY (idLeague)
);

CREATE TABLE IF NOT EXISTS lorenzoalliot_coderhouse.teams(
  idTeam INTEGER NOT NULL,
  name VARCHAR(255),
  teamCode VARCHAR (5),
  logo VARCHAR(1000),
  Country VARCHAR (255),
  founded INTEGER,

  PRIMARY KEY (idTeam)
);

CREATE TABLE IF NOT EXISTS lorenzoalliot_coderhouse.venues(
  idVenue INTEGER NOT NULL,
  idTeam INTEGER NOT NULL,
  name VARCHAR(255),
  address VARCHAR(255),
  city VARCHAR(255),
  capacity INTEGER,
  surface VARCHAR(25),
  image VARCHAR(1000),

  PRIMARY KEY (idVenue)
);

CREATE TABLE IF NOT EXISTS lorenzoalliot_coderhouse.countrys(
  idCountry INTEGER NOT NULL,
  codeCountry VARCHAR(3),
  name VARCHAR (255),
  flag VARCHAR (1000),

  PRIMARY KEY (idCountry)
);

CREATE TABLE IF NOT EXISTS lorenzoalliot_coderhouse.standings(
  idStanding INTEGER NOT NULL,
  idLeague INTEGER,
  rank INTEGER,
  idTeam INTEGER,
  points INTEGER,
  goalsDiff INTEGER,
  groupLeague VARCHAR (255),
  HomePlayed INTEGER,
  HomeWins INTEGER,
  HomeDraws INTEGER,
  HomeLosts INTEGER,
  HomeGoalsFor INTEGER,
  HomeGoalsAgainst INTEGER,
  AwayPlayed INTEGER,
  AwayWins INTEGER,
  AwayDraws INTEGER,
  AwayLosts INTEGER,
  AwayGoalsFor INTEGER,
  AwayGoalsAgainst INTEGER,
  TotalPlayed INTEGER,
  TotalWins INTEGER,
  TotalDraws INTEGER,
  TotalLosts INTEGER,
  TotalGoalsFor INTEGER,
  TotalGoalsAgainst INTEGER,

  PRIMARY KEY (idStanding)
)
  DISTKEY (idTeam) -- Definimos idTeam como la distkey.
  SORTKEY (rank); -- Definimos rank como la sortkey.
;
             """)
    pass



# Función para cargar datos en una tabla de la base de datos
def load_data_into_db(df, table_name, conn):
    print(f"Cargando datos en {table_name} en Redshift...")
    df.to_sql(table_name, conn, if_exists="replace", index=False)


# Crea el objeto DAG
dag = DAG(
    dag_id='proyectofinal_alliot',
    description='Un DAG para obtener y cargar datos',
    start_date=datetime(2023, 9, 23),
    schedule_interval='@daily',  # Define la frecuencia de ejecución
)

# Define tareas como operadores Python


extract_task = PythonOperator(
    task_id='extract_and_process_data',
    python_callable=extract_and_process_data,
    dag=dag,
)

# Crear conexión a la base de datos
def create_db(dbschema):
    conn_string = create_db_connection_string()
    conn = sa.create_engine(conn_string, connect_args={'options': f'-csearch_path={dbschema}'})
    return conn

db_conn_task = PythonOperator(
    task_id='create_db_connection',
    python_callable=create_db_connection_string,  # Llama a la función de cadena de conexión
    provide_context=True,  # Esto permite que Airflow pase el contexto a la función
    dag=dag,
)

# Crear tablas en la base de datos
def create_tables(conn_string, dbschema):
    conn = sa.create_engine(conn_string, connect_args={'options': f'-csearch_path={dbschema}'})
    create_tables_in_db(conn)

create_tables_task = PythonOperator(
    task_id='create_tables_in_db',
    python_callable=create_tables,
    op_args=create_db_connection_string(),
    dag=dag,
)

# Cargar datos en la base de datos
def load_data(conn_string, dbschema):
    df_league, df_team, df_venue, df_country, df_standings = extract_and_process_data()
    conn = sa.create_engine(conn_string, connect_args={'options': f'-csearch_path={dbschema}'})
    load_data_into_db(df_league, 'leagues', conn)
    load_data_into_db(df_team, 'teams', conn)
    load_data_into_db(df_venue, 'venues', conn)
    load_data_into_db(df_country, 'countrys', conn)
    load_data_into_db(df_standings, 'standings', conn)

load_data_task = PythonOperator(
    task_id='load_data_into_db',
    python_callable=load_data,
    op_args=create_db_connection_string(),  # Pasa la cadena de conexión
    dag=dag,
)

# Define el orden de ejecución de las tareas
extract_task >> db_conn_task >> create_tables_task >> load_data_task
