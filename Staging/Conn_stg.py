from config import read_config
import sqlalchemy as sa

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

# Crear conexión a la base de datos
def create_db(dbschema):
    conn_string = create_db_connection_string()
    conn = sa.create_engine(conn_string, connect_args={'options': f'-csearch_path={dbschema}'})
    return conn



# Función para crear tablas en la base de datos
def create_tables_stg_in_db(conn):
  conn.execute('''

    begin transaction;
    DROP TABLE IF EXISTS stg_countrys;
    CREATE TABLE IF NOT EXISTS lorenzoalliot_coderhouse.stg_countrys (
    idCountry INTEGER NOT NULL,
    codeCountry VARCHAR(3),
    name VARCHAR (255),
    flag VARCHAR (1000),

    PRIMARY KEY (idCountry)
    );


    DROP TABLE IF EXISTS stg_league;
    CREATE TABLE IF NOT EXISTS lorenzoalliot_coderhouse.stg_league (
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


    DROP TABLE IF EXISTS stg_teams;
    CREATE TABLE IF NOT EXISTS lorenzoalliot_coderhouse.stg_teams (
    idTeam INTEGER NOT NULL,
    name VARCHAR(255),
    teamCode VARCHAR (5),
    logo VARCHAR(1000),
    Country VARCHAR (255),
    founded INTEGER,

    PRIMARY KEY (idTeam)
  );


    DROP TABLE IF EXISTS stg_venues;
    CREATE TABLE IF NOT EXISTS lorenzoalliot_coderhouse.stg_venues (
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
    end transaction
             ''')
  pass

def create_tables_final_in_db(conn):
  conn.execute('''

    begin transaction;

    DROP TABLE IF EXISTS final_countrys;
    CREATE TABLE final_countrys (LIKE stg_countrys);



    DROP TABLE IF EXISTS final_league;
    CREATE TABLE final_league (LIKE stg_league);



    DROP TABLE IF EXISTS final_teams;
    CREATE TABLE final_teams (LIKE stg_teams);



    DROP TABLE IF EXISTS final_venues;
    CREATE TABLE final_venues (LIKE stg_venues);


    DROP TABLE IF EXISTS final_standings;
    CREATE TABLE IF NOT EXISTS lorenzoalliot_coderhouse.final_standings (
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
    end transaction
               ''')
  pass
  
# Crear tablas en la base de datos
def create_tables(conn_string, dbschema):
    conn = sa.create_engine(conn_string, connect_args={'options': f'-csearch_path={dbschema}'})
    create_tables_stg_in_db(conn)
    create_tables_final_in_db(conn)
