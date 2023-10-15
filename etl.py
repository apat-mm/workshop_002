import pandas as pd
import json
import logging
import database2

def read_csv():
    df_spotify = pd.read_csv("./spotify_dataset.csv")
    logging.info("Extracción finalizada")
    logging.debug('Los datos extraídos son: ', df_spotify)
    return df_spotify.to_json(orient='records')

def transform_csv(**kwargs):
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="read_csv"))
    df_spotify = pd.json_normalize(data=json_data)

    df_spotify = df_spotify.dropna()
    #df_spotify['duration_ms'] = df_spotify['duration_ms'] / 1000

    df_spotify['mins'] = df_spotify['duration_ms'] // 60000
    df_spotify['segs'] = (df_spotify['duration_ms'] % 60000) / 1000 
    df_spotify['duration_mmss'] = df_spotify.apply(lambda row: '{:02}:{:02}'.format(int(row['mins']), int(row['segs'])), axis=1)

    df_spotify = df_spotify.drop(["Unnamed: 0", "key", "loudness", "valence", "mode", "acousticness", "liveness", "time_signature", "mins", "segs", "duration_ms"], axis=1)

    df_spotify = df_spotify.rename(columns={'min_seg': 'duration(mm:ss)'})

    rangos = [0, 0.33, 0.66, 1.0]
    categorias = ['No bailable', 'Poco bailable', 'Bailable']
    df_spotify['danceability'] = pd.cut(df_spotify['danceability'], bins=rangos, labels=categorias, right=False)

    logging.info(f"Los datos transformados son: {df_spotify}")


    return df_spotify.to_json(orient='records')

def read_db():
    df_grammy = database2.data_db()
    logging.info("Extracción finalizada")
    logging.debug('Los datos extraídos son: ', df_grammy)
    return df_grammy.to_json(orient='records')

def transform_db(**kwargs):
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="read_db"))
    df_grammy = pd.json_normalize(data=json_data)

    df_grammy = df_grammy.drop(["published_at", "updated_at"], axis=1)
    df_grammy = df_grammy.rename(columns={'nominee': 'track'})
    df_grammy = df_grammy.rename(columns={'winner': 'nominee'})
    df_grammy['year'] = df_grammy['year'].astype(str)
    df_grammy = df_grammy.dropna(subset=['artist'])

    logging.info(f"Los datos transformados son: {df_grammy}")
    return df_grammy.to_json(orient='records')

def merge(**kwargs):
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="transform_db"))
    df_grammy = pd.json_normalize(data=json_data)

    json_data = json.loads(ti.xcom_pull(task_ids="transform_csv"))
    df_spotify = pd.json_normalize(data=json_data)

    df_final = df_spotify.merge(df_grammy, how="inner", left_on='track_name', right_on='track')
    df_final = df_final.drop(["track", "artist"], axis=1)


    logging.info("Se ha realizado la fusión de datos con éxito.")
    logging.info(f"Los datos finales son: {df_final}")
    return df_final.to_json(orient='records')

def load(**kwargs):
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="merge"))
    data = pd.json_normalize(data=json_data)

    logging.info("Cargando datos...")
    
    database2.load_final(data)

    logging.info("Los datos se han cargado en: tracks")

def store(**kwargs):
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="merge"))
    data = pd.json_normalize(data=json_data)
    data.to_csv('tracks.csv')
    logging.info("Archivo 'data.csv' almacenado y subido a Google Drive.")





