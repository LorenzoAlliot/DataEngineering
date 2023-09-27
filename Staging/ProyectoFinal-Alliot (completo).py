import pandas as pd
import configparser
import requests
import sqlalchemy as sa
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime , date, time
import os

# Función para obtener los datos de conexión desde un archivo remoto
def read_config():
    # # Obtener la ruta absoluta del archivo apikey.ini
    # ruta_archivo = os.path.abspath('apikey.ini')
    
    # # Leer el archivo local
    # with open(ruta_archivo, 'r') as archivo:
    #     contenido = archivo.read()
    
    # Parsear el contenido
    config = configparser.ConfigParser()
    config.read_string('/otp/airflow/config/config.ini')
    
    return config

# Función para obtener la clave de la API
def get_api_key():
    conn_data = read_config()
    return conn_data["API-SPORT"]["apikey"]


# Función para establecer las cabeceras y la URL de la API
def set_api_headers():
    key = get_api_key()
    headers = {
        'x-rapidapi-key': key,
        'x-rapidapi-host': 'v3.football.api-sports.io'
    }
    url = "https://v3.football.api-sports.io"
    return url, headers


# Función para realizar una solicitud a la API y devolver la respuesta JSON
def make_api_request(endpoint, params={}):
    url, headers = set_api_headers()
    response = requests.get(url + endpoint, headers=headers, params=params)
    return response.json()

def filter_and_process_leagues_data(leagues):
    leagues_true = []

    for league in leagues['response']:
        if league['seasons'][0]['coverage']['standings'] and league['country']['name'] == 'Argentina':
            league_info = {
                'league': league['league']['id'],
                'season': league['seasons'][0]['year']
            }
            leagues_true.append(league_info)

    return leagues_true


def get_team_data(leagues_true):
    url, headers = set_api_headers()
    endpoint = "/teams"
    responses_team = []

    for league in leagues_true:
        params = {"league": league["league"], "season": league["season"]}
        response_team = make_api_request(endpoint, params)
        responses_team.append(response_team)

    return responses_team


def get_standings_data(leagues_true):
    url, headers = set_api_headers()
    endpoint = "/standings"
    responses_standings = []

    for league in leagues_true:
        params = {"league": league["league"], "season": league["season"]}
        response_standings = make_api_request(endpoint, params)
        responses_standings.append(response_standings)

    return responses_standings



def process_data_to_dataframe(leagues, responses_team, responses_standings):
    # Transformación de dfLeague
    league_list = []
    for league in leagues['response']:
        league_info = {
            'idLeague': league['league']['id'],
            'name': league['league']['name'],
            'codeCountry': league['country']['code'],
            'logo': league['league']['logo'],
            'season': league['seasons'][0]['year'],
            'type': league['league']['type'],
            'startLeague': league['seasons'][0]['start'],
            'endLeague': league['seasons'][0]['end'],
        }
        league_list.append(league_info)
    dfLeague = pd.DataFrame(league_list)
    dfLeague['codeCountry'] = dfLeague['codeCountry'].fillna('WO')

    # Transformación de dfTeam
    team_list = []
    cantidad = 0
    for team in responses_team:
        cantidad += team['results']
        for team2 in team['response']:
            team_info = {
                'idTeam': team2['team']['id'],
                'name': team2['team']['name'],
                'teamCode': team2['team']['code'],
                'Country': team2['team']['country'],
                'founded': team2['team']['founded'],
                'logo': team2['team']['logo']
            }
            team_list.append(team_info)
    print(f'La cantidad de equipos es: {cantidad}')
    dfTeam = pd.DataFrame(team_list)
    dfTeam['founded'] = dfTeam['founded'].fillna(0)
    dfTeam['founded'] = dfTeam['founded'].astype(int)

    # Transformación de dfVenue
    venue_list = []
    cantidad = 0
    for venue in responses_team:
        cantidad += venue['results']
        for venue2 in venue['response']:
            venue_info = {
                'idTeam': venue2['team']['id'],
                'idVenue': venue2['venue']['id'],
                'name': venue2['venue']['name'],
                'address': venue2['venue']['address'],
                'city': venue2['venue']['city'],
                'capacity': venue2['venue']['capacity'],
                'surface': venue2['venue']['surface'],
                'image': venue2['venue']['image']
            }
            venue_list.append(venue_info)
    print(f'La cantidad de estadios es: {cantidad}')
    dfVenue = pd.DataFrame(venue_list)

    # Transformación de dfCountry
    country_list = []
    for country in leagues['response']:
        country_info = {
            'name': country['country']['name'],
            'codeCountry': country['country']['code'],
            'flag': country['country']['flag']
        }
        country_list.append(country_info)
    dfCountry = pd.DataFrame(country_list)
    dfCountry = dfCountry.drop_duplicates(subset='name').sort_values('name').reset_index(drop=True)
    dfCountry['codeCountry'] = dfCountry['codeCountry'].fillna("WO")

    # Transformación de dfStandings
    santandings_list = []
    id_counter = 1
    for standing in responses_standings:
        for standing2 in standing['response']:
            for standing3 in standing2['league']['standings']:
                for standing4 in standing3:
                    standing_info = {
                        'idStanding': id_counter,
                        'idLeague': standing2['league']['id'],
                        'rank': standing4['rank'],
                        'idTeam': standing4['team']['id'],
                        'points': standing4['points'],
                        'goalsDiff': standing4['goalsDiff'],
                        'groupLeague': standing4['group'],
                        'HomePlayed': standing4['home']['played'],
                        'HomeWins': standing4['home']['win'],
                        'HomeDraws': standing4['home']['draw'],
                        'HomeLosts': standing4['home']['lose'],
                        'HomeGoalsFor': standing4['home']['goals']['for'],
                        'HomeGoalsAgainst': standing4['home']['goals']['against'],
                        'AwayPlayed': standing4['away']['played'],
                        'AwayWins': standing4['away']['win'],
                        'AwayDraws': standing4['away']['draw'],
                        'AwayLosts': standing4['away']['lose'],
                        'AwayGoalsFor': standing4['away']['goals']['for'],
                        'AwayGoalsAgainst': standing4['away']['goals']['against'],
                        'TotalPlayed': standing4['all']['played'],
                        'TotalWins': standing4['all']['win'],
                        'TotalDraws': standing4['all']['draw'],
                        'TotalLosts': standing4['all']['lose'],
                        'TotalGoalsFor': standing4['all']['goals']['for'],
                        'TotalGoalsAgainst': standing4['all']['goals']['against']
                    }
                    id_counter += 1
                    santandings_list.append(standing_info)
    dfStandings = pd.DataFrame(santandings_list)

    return dfLeague, dfTeam, dfVenue, dfCountry, dfStandings


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

# Obtener datos y procesarlos
def extract_and_process_data():
    conn_data = read_config()
    api_key = get_api_key()
    headers = set_api_headers()
    leagues_data = make_api_request('/leagues')
    filtered_leagues = filter_and_process_leagues_data(leagues_data)
    team_data = get_team_data(filtered_leagues)
    standings_data = get_standings_data(filtered_leagues)
    df_league, df_team, df_venue, df_country, df_standings = process_data_to_dataframe(
        leagues_data, team_data, standings_data)
    return df_league, df_team, df_venue, df_country, df_standings

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
