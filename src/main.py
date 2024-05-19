# Config and imports
from helpers import Helpers
import requests
import pandas as pd
import json
import sqlalchemy as sa
from datetime import datetime

helpers = Helpers("config.ini")

# https://www.weatherapi.com/my/
# https://saturncloud.io/blog/how-to-convert-nested-json-to-pandas-dataframe-with-specific-format/
# https://api.weatherapi.com/v1/history.json?key=APIKEY&q=London&dt=2024-04-16&end_dt=2024-04-18
# print(pandas.__version__) # 2.1.4
# print(sa.__version__) # 1.4.52

# Get API Urls (list)
urls = helpers.createBasicAPIUrls()
current_date = datetime.now()
counter = 0

for url in urls:
    counter += 1

    # Get data from Weather API
    response = requests.get(url)

    # Transform data into a dict
    data = json.loads(response.content.decode('utf-8'))
    
    location_dataFrame = pd.json_normalize(data["location"])
    forecastDay_dataFrame = pd.json_normalize(data["forecast"]["forecastday"])
    hourForecast_dataFrame = pd.json_normalize(
        data["forecast"]["forecastday"],
        ["hour"],
        ["date"]
    )

    # Cleaning, adding and renaming columns
    location_dataFrame = location_dataFrame.drop("localtime_epoch", axis=1)
    location_dataFrame.columns = ['name', 'region', 'country', 'latitude', 'longitude', 'tz_id', 'localdate']
    location_dataFrame["load_date"] = current_date
    location_dataFrame["update_date"] = current_date
    location_id = location_dataFrame["tz_id"][0]

    forecastDay_dataFrame = forecastDay_dataFrame.drop(["hour", "date_epoch"], axis=1)
    forecastDay_dataFrame.columns = ['date', 'maxtemp_c', 'maxtemp_f',
        'mintemp_c', 'mintemp_f', 'avgtemp_c', 'avgtemp_f',
        'maxwind_mph', 'maxwind_kph', 'totalprecip_mm',
        'totalprecip_in', 'totalsnow_cm', 'avgvis_km',
        'avgvis_miles', 'avghumidity', 'daily_will_it_rain',
        'daily_chance_of_rain', 'daily_will_it_snow',
        'daily_chance_of_snow', 'condition_text', 'condition_icon',
        'condition_code', 'uv', 'sunrise', 'sunset',
        'moonrise', 'moonset', 'moon_phase',
        'moon_illumination']
    forecastDay_dataFrame["load_date"] = current_date
    forecastDay_dataFrame["update_date"] = current_date
    forecastDay_dataFrame["location_id"] = location_id

    hourForecast_dataFrame = hourForecast_dataFrame.drop("time_epoch", axis=1)
    hourForecast_dataFrame.columns = ['time', 'temp_c', 'temp_f', 'is_day', 'wind_mph',
        'wind_kph', 'wind_degree', 'wind_dir', 'pressure_mb', 'pressure_in',
        'precip_mm', 'precip_in', 'snow_cm', 'humidity', 'cloud', 'feelslike_c',
        'feelslike_f', 'windchill_c', 'windchill_f', 'heatindex_c',
        'heatindex_f', 'dewpoint_c', 'dewpoint_f', 'will_it_rain',
        'chance_of_rain', 'will_it_snow', 'chance_of_snow', 'vis_km',
        'vis_miles', 'gust_mph', 'gust_kph', 'uv', 'condition_text',
        'condition_icon', 'condition_code', 'date']
    hourForecast_dataFrame["load_date"] = current_date
    hourForecast_dataFrame["update_date"] = current_date
    hourForecast_dataFrame["location_id"] = location_id

    ## Save data into a file
    # f = open("data.json", "x")
    # f.write(response.decode('utf-8'))

    # Save data into Redshift

    conn, engine = helpers.connectToDB()

    # location_dataFrame.to_sql(
    #     name = 'locations',
    #     con = conn,
    #     schema = "angelmamberto15_coderhouse",
    #     if_exists = 'replace' if counter == 1 else 'append',
    #     method = 'multi',
    #     chunksize = 1000,
    #     index = False
    # )

    # forecastDay_dataFrame.to_sql(
    #     name = 'forecastsday',
    #     con = conn,
    #     schema = "angelmamberto15_coderhouse",
    #     if_exists = 'replace' if counter == 1 else 'append',
    #     method = 'multi',
    #     chunksize = 1000,
    #     index = False
    # )

    # hourForecast_dataFrame.to_sql(
    #     name = 'hourforecasts',
    #     con = conn,
    #     schema = "angelmamberto15_coderhouse",
    #     if_exists = 'replace' if counter == 1 else 'append',
    #     method = 'multi',
    #     chunksize = 1000,
    #     index = False
    # )


    df_Nuevo = location_dataFrame.merge(forecastDay_dataFrame, left_on='location_id', right_on='location_id', how='left')
    df_Nuevo2 = df_Nuevo.merge(hourForecast_dataFrame, left_on='location_id', right_on='location_id', how='left')

    print(df_Nuevo2)


    df_Nuevo2.to_sql(
        name = 'dfNuevo',
        con = conn,
        schema = "angelmamberto15_coderhouse",
        if_exists = 'replace',
        method = 'multi',
        chunksize = 1000,
        index = False
    )