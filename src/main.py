# Config and imports
import helpers

config = helpers.readConfig("config.ini")

# Levantar datos de la API
# https://www.weatherapi.com/my/
# https://api.weatherapi.com/v1/history.json?key=f80ece51f831494b981182127241904&q=London&dt=2024-04-16&end_dt=2024-04-18

url = helpers.createBasicAPIUrl(config)



# Meter datos en Redshift
