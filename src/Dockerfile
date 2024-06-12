# Imagen de Python
FROM python:3.8

# Setea el directorio de trabajo
WORKDIR /dags

# Copia en /app el archivo
COPY /dags .

# Instala Airflow, Pandas y SQL Alchemy
RUN pip install airflow==2.6
RUN pip install pandas==2.1.4
RUN pip install sqlalchemy==1.4.52

# Comando que ejecuta al hacer docker run
CMD ["python", "-u", "main.py"]

# docker-compose up airflow-init
# docker-compose up
