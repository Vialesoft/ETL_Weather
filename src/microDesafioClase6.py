# Microdesafío 30/04/2024

# import pytrends
import matplotlib.pyplot as pyplot
import pandas
# import lxml

from pytrends.request import TrendReq

terms = ["Fin del mundo", "Calentamiento global", "Terremoto", "Tsunami"]
pytrends = TrendReq(hl='en-US', tz=360)
pytrends.build_payload(terms, cat=0, timeframe='today 5-y', gprop='')

results = pytrends.interest_over_time()[terms]
print(results.head())

print(results.plot(kind='line',figsize=(12,6), xlabel='Fecha',ylabel='Interés de audiencia',title='Interés en el tiempo'))

# Open a new window with the plot
pyplot.show()