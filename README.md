# ETL_Weather

 Coder House Data Engineering Course 2024 (Final Project)
 
- To run the code, use the command **python main.py** on directory **src**

## config.ini file

**Structure of config.ini file:**

Names of sections are important, keep them as they are here

### Sections

__[APIConnection]__

**apiKey** = your_weatherapi_key

**apiUrl** = URL of API. E.g.: http://api.weatherapi.com/v1/

**apiMethod** = Method you want to use. E.g.: history.json

**daysHistory** = How many days in the past program will query

**apiCities** = Cities comma separated. E.g.: London, Montevideo

---

__[Database]__

**host** = Amazon Redshift link (or another provider)

**dbName** = Name of database where you want to bulk DataFrame data

**port** = Amazon Redshift (or another provider) port connection

**user** = User to connect to DB

**schema** = Schema into Redshift DB

**password** = Password to connect to DB

---

### WeatherAPI Documentation

https://www.weatherapi.com/docs/