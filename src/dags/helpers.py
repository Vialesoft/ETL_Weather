import configparser
from classes import ApiConnectionConfig, DatabaseConfig, EmailConnection
from datetime import datetime, timedelta
import sqlalchemy as sa
import psycopg2

class Helpers:
  def __init__(self, configPath):
        self.configPath = configPath
        self.parser = configparser.ConfigParser()
        self.parser.sections()

  def __get_API_Connection_Config(self, fileName):
      self.parser.read(fileName)
      config = self.parser["APIConnection"]

      configRet = ApiConnectionConfig()
      configRet.apiKey = config["apiKey"]
      configRet.apiUrl = config["apiUrl"]
      configRet.apiMethod = config["apiMethod"]
      configRet.apiCities = config["apiCities"].split(',')
      configRet.daysHistory = int(config["daysHistory"])

      return configRet

  def __get_Database_Config(self, fileName):
      self.parser.read(fileName)
      config = self.parser["Database"]

      configRet = DatabaseConfig()
      configRet.host = config["host"]
      configRet.dbName = config["dbName"]
      configRet.schema = config["schema"]
      configRet.port = config["port"]
      configRet.user = config["user"]
      configRet.password = config["password"]

      return configRet

  def __build_conn_string(self):
    config = self.__get_Database_Config(self.configPath)

    # Contruye la cadena de conexión
    conn_string =f'postgresql://{config.user}:{config.password}@{config.host}:{config.port}/{config.dbName}?sslmode=require'
    # print(conn_string)
    return conn_string

  # -- #

  def createBasicAPIUrls(self):
      config = self.__get_API_Connection_Config(self.configPath)

      dateFrom = (datetime.now() - timedelta(days=config.daysHistory)).strftime('%Y-%m-%d')
      dateTo = datetime.now().strftime('%Y-%m-%d')
      urls = []

      for city in config.apiCities:
        urls.append(config.apiUrl + config.apiMethod + "?key=" + config.apiKey + "&q=" + city + "&dt=" + dateFrom + "&end_dt=" + dateTo)

      return urls

  def connectToDB(self):
    # Crea una conexión a la base de datos
    conn_string = self.__build_conn_string()

    engine = sa.create_engine(conn_string)
    conn = engine.connect()

    return conn, engine

  def connectToDBPsycopg(self):
    # Crea una conexión a la base de datos
    conn = psycopg2.connect(self.__build_conn_string())

    return conn

  def get_Email_Config(self):
      self.parser.read(self.configPath)
      config = self.parser["MailServer"]

      configRet = EmailConnection()
      configRet.url = config["url"]
      configRet.email = config["email"]
      configRet.emailPassword = config["emailPassword"]
      configRet.emailDest = config["emailDest"]

      return configRet
