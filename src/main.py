# Config and imports
import helpers
import requests
import pandas
import io
import json

config = helpers.readConfig("config.ini")

# Levantar datos de la API
# https://www.weatherapi.com/my/
# https://api.weatherapi.com/v1/history.json?key=f80ece51f831494b981182127241904&q=London&dt=2024-04-16&end_dt=2024-04-18

url = helpers.createBasicAPIUrl(config)

response = requests.get(url).content
# print(response)

# print(response.json())
# a = pandas.read_csv(io.StringIO(response.decode('utf-8')))

# print(response.json())

po = json.loads(response.decode('utf-8'))
location = po["location"]
forecasts = po["forecast"]

# print(location)
# print(forecasts)

# f = open("lala.json", "x")
# f.write(response.decode('utf-8'))
# a = pandas.read_json("lala.json")
# print(a)

f = open("files/location.json", "x")
f2 = open("files/forecasts.json", "x")
f.write(json.dumps(location))
f2.write(json.dumps(forecasts))

# Meter datos en Redshift
