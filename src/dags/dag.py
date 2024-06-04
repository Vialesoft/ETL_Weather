from datetime import datetime, timedelta
from email.policy import default
from airflow import DAG
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from helpers import Helpers
from classes import ApiConnectionConfig
from classes import DatabaseConfig
from main import WeatherETL

# Config and imports
import requests
import pandas as pd
import json
import sqlalchemy as sa
from datetime import datetime, timedelta
import psycopg2
import configparser

default_args= {
    'owner': 'Angel Mamberto',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

# def runETL():
#     helpers = Helpers("config.ini")

#     # https://www.weatherapi.com/my/
#     # https://saturncloud.io/blog/how-to-convert-nested-json-to-pandas-dataframe-with-specific-format/
#     # https://api.weatherapi.com/v1/history.json?key=APIKEY&q=London&dt=2024-04-16&end_dt=2024-04-18
#     # print(pandas.__version__) # 2.1.4
#     # print(sa.__version__) # 1.4.52

#     # Get API Urls (list)
#     urls = helpers.createBasicAPIUrls()
#     current_date = datetime.now()
#     counter = 0

#     connectionPsycopg = helpers.connectToDBPsycopg()

#     with connectionPsycopg:
#         with connectionPsycopg.cursor() as cur:
#             cur.execute(f"TRUNCATE TABLE forecast;")
#     connectionPsycopg.close()

#     for url in urls:
#         counter += 1

#         # Get data from Weather API
#         response = requests.get(url)

#         # Transform data into a dict
#         data = json.loads(response.content.decode('utf-8'))
            
#         location_dataFrame = pd.json_normalize(data["location"])
#         forecastDay_dataFrame = pd.json_normalize(data["forecast"]["forecastday"])
#         hourForecast_dataFrame = pd.json_normalize(
#             data["forecast"]["forecastday"],
#             ["hour"],
#             ["date"]
#         )

#         # Cleaning, adding and renaming columns
#         location_dataFrame = location_dataFrame.drop("localtime_epoch", axis=1)
#         location_dataFrame.columns = ['name', 'region', 'country', 'latitude', 'longitude', 'tz_id', 'localdate']
#         location_dataFrame["load_date"] = current_date
#         location_dataFrame["update_date"] = current_date
#         location_id = location_dataFrame["tz_id"][0]

#         forecastDay_dataFrame = forecastDay_dataFrame.drop(["hour", "date_epoch"], axis=1)
#         forecastDay_dataFrame.columns = ['date', 'maxtemp_c', 'maxtemp_f',
#             'mintemp_c', 'mintemp_f', 'avgtemp_c', 'avgtemp_f',
#             'maxwind_mph', 'maxwind_kph', 'totalprecip_mm',
#             'totalprecip_in', 'totalsnow_cm', 'avgvis_km',
#             'avgvis_miles', 'avghumidity', 'daily_will_it_rain',
#             'daily_chance_of_rain', 'daily_will_it_snow',
#             'daily_chance_of_snow', 'condition_text', 'condition_icon',
#             'condition_code', 'uv', 'sunrise', 'sunset',
#             'moonrise', 'moonset', 'moon_phase',
#             'moon_illumination']
#         forecastDay_dataFrame["load_date"] = current_date
#         forecastDay_dataFrame["update_date"] = current_date
#         forecastDay_dataFrame["location_id"] = location_id

#         hourForecast_dataFrame = hourForecast_dataFrame.drop("time_epoch", axis=1)
#         hourForecast_dataFrame.columns = ['time', 'temp_c', 'temp_f', 'is_day', 'wind_mph',
#             'wind_kph', 'wind_degree', 'wind_dir', 'pressure_mb', 'pressure_in',
#             'precip_mm', 'precip_in', 'snow_cm', 'humidity', 'cloud', 'feelslike_c',
#             'feelslike_f', 'windchill_c', 'windchill_f', 'heatindex_c',
#             'heatindex_f', 'dewpoint_c', 'dewpoint_f', 'will_it_rain',
#             'chance_of_rain', 'will_it_snow', 'chance_of_snow', 'vis_km',
#             'vis_miles', 'gust_mph', 'gust_kph', 'uv', 'condition_text',
#             'condition_icon', 'condition_code', 'date']
#         hourForecast_dataFrame["load_date"] = current_date
#         hourForecast_dataFrame["update_date"] = current_date
#         hourForecast_dataFrame["location_id"] = location_id
            
#         # Merging dataframes 

#         df_Nuevo = location_dataFrame.merge(forecastDay_dataFrame, left_on='tz_id', right_on='location_id', how='left')
#         df_Nuevo2 = df_Nuevo.merge(hourForecast_dataFrame, left_on='location_id', right_on='location_id', how='left')

#         # Save data into Redshift
#         conn, engine = helpers.connectToDB()

#         df_Nuevo2.to_sql(
#             name = 'forecast',
#             con = conn,
#             schema = "angelmamberto15_coderhouse",
#             if_exists = 'append',
#             method = 'multi',
#             chunksize = 1000,
#             index = False
#         )

# class ApiConnectionConfig:
#     def __init__(self):
#         apiKey = ""
#         apiUrl = ""
#         apiMethod = ""
#         apiCities = []
#         daysHistory = 0

# class DatabaseConfig:
#     def __init__(self):
#         host = ""
#         dbName = ""
#         port = ""
#         schema = ""
#         user = ""
#         password = ""

# class Helpers:
#   def __init__(self, configPath):
#         self.configPath = configPath
#         print(" HOLA " + configPath)
#         self.parser = configparser.ConfigParser()
#         self.parser.sections()

#   def __get_API_Connection_Config(self, fileName):
#       self.parser.read(fileName)

#       print(" HOLA 2 " + 'pepe'.join(self.parser.sections()))
#       config = self.parser["APIConnection"]

#       configRet = ApiConnectionConfig()
#       configRet.apiKey = config["apiKey"]
#       configRet.apiUrl = config["apiUrl"]
#       configRet.apiMethod = config["apiMethod"]
#       configRet.apiCities = config["apiCities"].split(',')
#       configRet.daysHistory = int(config["daysHistory"])

#       return configRet

#   def __get_Database_Config(self, fileName):
#       self.parser.read(fileName)
#       config = self.parser["Database"]

#       configRet = DatabaseConfig()
#       configRet.host = config["host"]
#       configRet.dbName = config["dbName"]
#       configRet.schema = config["schema"]
#       configRet.port = config["port"]
#       configRet.user = config["user"]
#       configRet.password = config["password"]

#       return configRet

#   def __build_conn_string(self):
#     config = self.__get_Database_Config(self.configPath)

#     # Contruye la cadena de conexión
#     conn_string =f'postgresql://{config.user}:{config.password}@{config.host}:{config.port}/{config.dbName}?sslmode=require'
#     # print(conn_string)
#     return conn_string

#   # -- #

#   def createBasicAPIUrls(self):
#       config = self.__get_API_Connection_Config(self.configPath)

#       dateFrom = (datetime.now() - timedelta(days=config.daysHistory)).strftime('%Y-%m-%d')
#       dateTo = datetime.now().strftime('%Y-%m-%d')
#       urls = []

#       for city in config.apiCities:
#         urls.append(config.apiUrl + config.apiMethod + "?key=" + config.apiKey + "&q=" + city + "&dt=" + dateFrom + "&end_dt=" + dateTo)

#       return urls

#   def connectToDB(self):
#     # Crea una conexión a la base de datos
#     conn_string = self.__build_conn_string()

#     engine = sa.create_engine(conn_string)
#     conn = engine.connect()

#     return conn, engine

#   def connectToDBPsycopg(self):
#     # Crea una conexión a la base de datos
#     conn = psycopg2.connect(self.__build_conn_string())

#     return conn

with DAG(
    default_args=default_args,
    dag_id='ETL_Weather_DAG',
    description= 'Dag del proyecto final',
    start_date=datetime(2024,6,2),
    schedule_interval='0 0 * * *'
    ) as dag:
    # task1= PostgresOperator(
    #     task_id='crear_tabla_postgres',
    #     postgres_conn_id= 'postgres_localhost',
    #     sql="""
    #         CREATE TABLE forecast (
    #             name            TEXT,
    #             region          TEXT,
    #             country         TEXT,
    #             latitude        FLOAT(53),
    #             longitude       FLOAT(53),
    #             tz_id           TEXT NOT NULL,
    #             localdate       TEXT,
    #             load_date_x     TIMESTAMP WITHOUT TIME ZONE,
    #             update_date_x   TIMESTAMP WITHOUT TIME ZONE,
    #             date_x          TEXT,
    #             maxtemp_c       FLOAT(53),
    #             maxtemp_f       FLOAT(53),
    #             mintemp_c       FLOAT(53),
    #             mintemp_f       FLOAT(53),
    #             avgtemp_c       FLOAT(53),
    #             avgtemp_f       FLOAT(53),
    #             maxwind_mph     FLOAT(53),
    #             maxwind_kph     FLOAT(53),
    #             totalprecip_mm  FLOAT(53),
    #             totalprecip_in  FLOAT(53),
    #             totalsnow_cm    FLOAT(53),
    #             avgvis_km       FLOAT(53),
    #             avgvis_miles    FLOAT(53),
    #             avghumidity     BIGINT,
    #             daily_will_it_rain      BIGINT,
    #             daily_chance_of_rain    BIGINT,
    #             daily_will_it_snow      BIGINT,
    #             daily_chance_of_snow    BIGINT,
    #             condition_text_x        TEXT,
    #             condition_icon_x        TEXT,
    #             condition_code_x        BIGINT,
    #             uv_x            FLOAT(53),
    #             sunrise         TEXT,
    #             sunset          TEXT,
    #             moonrise        TEXT,
    #             moonset         TEXT,
    #             moon_phase      TEXT,
    #             moon_illumination       BIGINT,
    #             load_date_y     TIMESTAMP WITHOUT TIME ZONE,
    #             update_date_y   TIMESTAMP WITHOUT TIME ZONE,
    #             location_id     TEXT,
    #             time            TEXT NOT NULL,
    #             temp_c          FLOAT(53),
    #             temp_f          FLOAT(53),
    #             is_day          BIGINT,
    #             wind_mph        FLOAT(53),
    #             wind_kph        FLOAT(53),
    #             wind_degree     BIGINT,
    #             wind_dir        TEXT,
    #             pressure_mb     FLOAT(53),
    #             pressure_in     FLOAT(53),
    #             precip_mm       FLOAT(53),
    #             precip_in       FLOAT(53),
    #             snow_cm         FLOAT(53),
    #             humidity        BIGINT,
    #             cloud           BIGINT,
    #             feelslike_c     FLOAT(53),
    #             feelslike_f     FLOAT(53),
    #             windchill_c     FLOAT(53),
    #             windchill_f     FLOAT(53),
    #             heatindex_c     FLOAT(53),
    #             heatindex_f     FLOAT(53),
    #             dewpoint_c      FLOAT(53),
    #             dewpoint_f      FLOAT(53),
    #             will_it_rain    BIGINT,
    #             chance_of_rain  BIGINT,
    #             will_it_snow    BIGINT,
    #             chance_of_snow  BIGINT,
    #             vis_km          FLOAT(53),
    #             vis_miles       FLOAT(53),
    #             gust_mph        FLOAT(53),
    #             gust_kph        FLOAT(53),
    #             uv_y            FLOAT(53),
    #             condition_text_y        TEXT,
    #             condition_icon_y        TEXT,
    #             condition_code_y        BIGINT,
    #             date_y          TEXT,
    #             load_date       TIMESTAMP WITHOUT TIME ZONE,
    #             update_date     TIMESTAMP WITHOUT TIME ZONE,
    #             PRIMARY KEY (tz_id, time)
    #         )
    #     """
    # )
    task2 =PythonOperator(
        task_id = 'Correr_ETL',
        python_callable = WeatherETL.runETL,
    )
    # task1 >> task2

    task2