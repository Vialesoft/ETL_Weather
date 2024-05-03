import configparser
from classes import ApiConnectionConfig, DatabaseConfig
import sqlalchemy as sa

parser = configparser.ConfigParser()
parser.sections()

def getAPIConnectionConfig(fileName):
    parser.read(fileName)
    config = parser["APIConnection"]

    configRet = ApiConnectionConfig()
    configRet.apiKey = config["apiKey"]
    configRet.apiUrl = config["apiUrl"]
    configRet.apiMethod = config["apiMethod"]
    configRet.apiCities = config["apiCities"]

    return configRet

def getDatabaseConfig(fileName):
    parser.read(fileName)
    config = parser["Database"]

    configRet = DatabaseConfig()
    configRet.host = config["host"]
    configRet.dbName = config["dbName"]
    configRet.schema = config["schema"]
    configRet.port = config["port"]
    configRet.user = config["user"]
    configRet.password = config["password"]

    return configRet

def createBasicAPIUrl(config:ApiConnectionConfig):
    return config.apiUrl + config.apiMethod + "?key=" + config.apiKey + "&q=" + config.apiCity + "&dt=2024-04-16&end_dt=2024-04-18"

def build_conn_string(config_path):
  config = getDatabaseConfig(config_path)

  # Contruye la cadena de conexión
  conn_string =f'postgresql://{config.user}:{config.password}@{config.host}:{config.port}/{config.dbName}?sslmode=require'
  # print(conn_string)
  return conn_string

def connect_to_db(conn_string):
  # Crea una conexión a la base de datos

  engine = sa.create_engine(conn_string)
  conn = engine.connect()

  return conn, engine