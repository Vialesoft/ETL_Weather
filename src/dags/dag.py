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

default_args = {
    'owner': 'Angel Mamberto',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='ETL_Weather_DAG',
    description= 'Dag del proyecto final',
    start_date=datetime(2024,6,2),
    schedule_interval='0 0 * * *'
    ) as dag:
    task1 =PythonOperator(
        task_id = 'Correr_ETL',
        python_callable = WeatherETL.runETL,
    )

    task1