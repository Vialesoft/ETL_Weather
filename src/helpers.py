import configparser
from classes import ApiConnectionConfig

config = configparser.ConfigParser()
config.sections()

def readConfig(fileName):
    config.read(fileName)

    configRet = ApiConnectionConfig()
    configRet.apiKey = config["APIConnection"]["apiKey"]
    configRet.apiUrl = config["APIConnection"]["apiUrl"]
    configRet.apiMethod = config["APIConnection"]["apiMethod"]
    configRet.apiCity = config["APIConnection"]["apiCity"]

    return configRet

def createBasicAPIUrl(config:ApiConnectionConfig):
    return config.apiUrl + config.apiMethod + "?key=" + config.apiKey + "&q=" + config.apiCity + "&dt=2024-04-16&end_dt=2024-04-18"