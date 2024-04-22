# ETL_Weather

 Coder House Data Engineering Course 2024 (Final Project)

## config.ini file

**Structure of config.ini file:**

Names of sections are important, keep them as they are here

### Sections

__[APIConnection]__

apiKey = your_weatherapi_key

apiUrl = URL of API. E.g.: http://api.weatherapi.com/v1/

apiMethod = Method you want to use. E.g.: history.json

apiCity = City. E.g.: London

---

__[Database]__

host = Amazon Redshift link (or another provider)

dbName = Name of database where you want to bulk DataFrame data

port = Amazon Redshift (or another provider) port connection

user = User to connect to DB

password = Password to connect to DB
