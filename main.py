#Importación de las librerías necesarias.
from fastapi import FastAPI
import json
import pandas as pd
import numpy as np

#Le damos a FastApi un título, una descripción y una versión.
app = FastAPI()

df = pd.read_csv("streaming_ratings.csv") 

app = FastAPI(title='Proyecto Individual',
            description='Data Science',
            version='1.0.1')



#Primera función donde la API va a tomar mi dataframe para las consultas.
@app.get('/')
async def read_root():
    return {'Hola! Estas en mi primera API'}



#Función para que interactue mi servidor local
@app.get('/info')
async def index():
    return {'API realizada por Andres Baez'}

@app.get('/about/')
async def about():
    return {'Proyecto Individual de la cohorte 08 de Data Science'}




#Consigna número uno (01) película que más duró según año, plataforma y tipo de duración. -

#función que debe devolver la película o serie con duración máxima de acuerdo a las diferentes plataformas
# Consulta 1: Película con mayor duración con filtros opcionales de AÑO, PLATAFORMA Y TIPO DE DURACIÓN.
@app.get("/get_max_duration")
async def get_max_duration(year: int = None, platform: str = None, duration_type: str = None):
   # Filtrar las columnas
    filtered_df = df.loc[(df["year"] == year if year is not None else True) &
                         (df["platform"] == platform if platform is not None else True) &
                         (df["duration_type"] == duration_type if duration_type is not None else True)]

    # Verificar si el DataFrame filtrado está vacío
    if filtered_df.empty:
        return {"message": "No se encontró una película con los filtros proporcionados."}
    
    # Mostrar la película con la mayor duración
    max_duration_movie = filtered_df.loc[filtered_df["duration_int"].idxmax()]
    
    # Convertir la duración a un formato más legible
    duration_str = f'{max_duration_movie["duration_int"] // 60}h {max_duration_movie["duration_int"] % 60}min'
    max_duration_movie = max_duration_movie.copy()
    max_duration_movie["duration"] = duration_str
  

    return max_duration_movie.to_dict()






#Consigna número dos (02), cantidad de películas por plataforma con un puntaje mayor a XX en determinado año
@app.get("/get_score_count/")  # Decorador que indica la ruta y el método HTTP que manejará la función
async def get_score_count(platform:str, score:int, year:int):  # Definición de la función que toma tres parámetros llamados 'platform', 'score' y 'year'
   
    filtered = df.copy()  # Se crea una copia del DataFrame 'df' y se almacena en una variable llamada 'filtered'
    filtered = filtered[filtered['platform'] == platform]  # Se filtran los datos según el valor del parámetro 'platform'
    if year is not None:  # Se verifica si el valor del parámetro 'year' no es nulo
        filtered = filtered[filtered['year'] == year]  # Si no es nulo, se filtran los datos según el valor del parámetro 'year'
    
    count = len(filtered[filtered['scored'] > score])  # Se cuenta la cantidad de filas en las que la columna 'scored' es mayor que el valor del parámetro 'score'
    return {'count': count}  # Se retorna un diccionario con la cantidad de filas encontradas









#Consigna numero tres (03), cantidad de películas por plataforma con filtro de PLATAFORMA. -
@app.get('/count_platform/')  # Decorador que indica la ruta y el método HTTP que manejará la función
async def get_count_platform(platform: str):  # Definición de la función que toma un parámetro llamado 'platform'
    df = pd.read_csv('streaming_ratings.csv')  # Carga el archivo CSV en un DataFrame llamado 'df'
    count = len(df[df['platform'] == platform])  # Cuenta la cantidad de filas en las que la columna 'platform' coincide con el valor del parámetro
    return {'count': count}  # Retorna un diccionario con la cantidad de filas encontradas







#Consigna numero cuatro (04), actor que más se repite según plataforma y año. -
@app.get('/actor/')
async def get_actor(platform: str, year: int):
    # Filtrar por plataforma y año
    df_filtered = df[(df['platform'] == platform) & (df['year'] == year)]
    # Concatenar los valores de la columna "cast"
    actors_df = df_filtered['cast'].str.split(', ', expand=True)
    # Contar la frecuencia de cada actor
    actor_count = actors_df.stack().value_counts().reset_index()
    # Obtener el actor con mayor frecuencia
    most_common_actor = actor_count.iloc[0]['index']
    # Devolver el actor con mayor frecuencia
    return most_common_actor
