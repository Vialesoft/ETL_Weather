# https://www.kaggle.com/competitions/titanic/data

import pandas as pd

df = pd.read_csv("files/train.csv")
# Ver dataframe (reducido)
# print(df)

# Ver los tipos de dato de cada columna
# print(df.dtypes)

# Extraer columnas Edad y Nombre (otro dataframe)
df2 = df[["Name", "Age"]]
# print(df2)

# Extraer solo columna edad (tipo de dato serie)
df3 = df["Age"]
# print(df3)

# Tamaño Dataframe
# print(df.shape)
# print(df2.shape)
# print(df3.shape)

# Solo aquellas filas cuya edad sea mayor a 30
df4 = df[df["Age"] > 30]
# print(df4)
# print(df["Age"] > 30)
df_male = df[df["Sex"] == "male"]
# print(df_male)

df_class23 = df[df["Pclass"].isin([2,3])]
# print(df_class23)

df_class23_Expresivo = df[(df["Pclass"] == 2) | (df["Pclass"] == 3)]
# print(df_class23_Expresivo)

# Filtrar columnas (Todas las filas)
# print(df.loc[:, ["Age"]])
# print(df.loc[:, ["Age", "Sex"]])

# Solo fila 5
# print(df.loc[5, ["Age", "Sex"]])
# print(df.loc[5:5, ["Age", "Sex"]])

# Filtra las primeras cinco filas
# print(df.loc[5:, ["Age", "Sex"]])

# Sólo las primeras cinco
# print(df.loc[:5, ["Age", "Sex"]])

# Crear nueva columna (precio en pesos Mexicanos)
df["Fare_MXN"] = df["Fare"] * 20
# print(df)

def pay_if_rich(x):
    if x["Pclass"]  == 1:
        return x["Fare"] * 1.20
    
    return x["Fare"]

# Axis = 1 indica que se debe aplicar a las columnas
df["Fare_Rich_Tax"] = df.apply(pay_if_rich, axis=1)
# print(df)

# Funciones lambda
df["Fare_Rich_Tax_Lambda"] = df.apply(lambda x: x["Fare"] * 1.20 if x["Pclass"] == 1 else x["Fare"], axis = 1)
# print(df)

# Joins
df_aux = pd.DataFrame({"Pclass": [1,2,3], "Description": ["Upper", "Medium", "Lower"]})
# print(df_aux)

# Left Join (Pandas merge)
# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html
df_merge = df.merge(df_aux, how="left", on="Pclass")
# print(df_merge)

# Agrupar y agregar
df_Titanic = pd.read_csv("files/train.csv")

# Agrupa por sexo, y cuenta la cantidad de PassengerIds
# print(df.groupby(["Sex"]).PassengerId.count())

# Promedios
# print(df.groupby(["Pclass"]).Fare.mean())
# print(df.groupby(["Pclass"]).Age.mean())
# print(df["Age"].mean())

# print(df["Age"].max())
# print(df["Age"].min())

print(df.groupby(["Pclass"])[["Fare", "Age"]].mean())


