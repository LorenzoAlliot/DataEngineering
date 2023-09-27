# Este archivo importa la funcion read_config del script config para conectar a la API
# Se conecta a la API con las credenciales correspondientes y los headdes seteados
# Luego hace llamado a la API y extrae los datos
# Se les da formato a los datos y se crean data frames

from config import read_config
import pandas as pd
import requests
import sqlalchemy as sa

# Función para obtener la clave de la API
def get_api_key():
    conn_data = read_config()
    return conn_data["API-SPORT"]["apikey"]

# Función para setear los encabezados de la api
def set_api_headers():
    key = get_api_key()
    headers = {
        'x-rapidapi-key': key,
        'x-rapidapi-host': 'v3.football.api-sports.io'
    }
    url = "https://v3.football.api-sports.io"
    return url, headers

#-------------------------------- EXTRACCIÓN DE LA API ------------------------------------------

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

# --------------------------- TRANSFROMACIÓN ----------------------------------------------

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


#Se crea una funcion que utiliza todas las anteriores para luego usarse en una task del DAG

def extract_and_process_data():
    conn_data = read_config()
    api_key = get_api_key()
    headers = set_api_headers()
    leagues_data = make_api_request('/leagues')
    filtered_leagues = filter_and_process_leagues_data(leagues_data)
    team_data = get_team_data(filtered_leagues)
    standings_data = get_standings_data(filtered_leagues)
    dfLeague, dfTeam, dfVenue, dfCountry, dfStandings = process_data_to_dataframe(
        leagues_data, team_data, standings_data)
    return dfLeague, dfTeam, dfVenue, dfCountry, dfStandings

#-------------------------------------- CARGA DE DATOS -----------------------------------------------------

# Función para cargar datos en una tabla de la base de datos
def load_data_into_db(df, table_name, conn):
    print(f"Cargando datos en {table_name} en Redshift...")
    df.to_sql(table_name, conn, if_exists="append", index=False,method='multi')


# Cargar datos en la base de datos
def load_data(conn_string, dbschema):
    df_league, df_team, df_venue, df_country, df_standings = extract_and_process_data()
    conn = sa.create_engine(conn_string, connect_args={'options': f'-csearch_path={dbschema}'})
    load_data_into_db(df_league, 'leagues', conn)
    load_data_into_db(df_team, 'teams', conn)
    load_data_into_db(df_venue, 'venues', conn)
    load_data_into_db(df_country, 'countrys', conn)
    load_data_into_db(df_standings, 'standings', conn)
