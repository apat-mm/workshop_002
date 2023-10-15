import pandas as pd
import mysql.connector
import json

Archivo = '/home/vagrant/workshop/the_grammy_awards.csv'
with open('db_config.json') as f:
    dbfile = json.load(f)
    db_config = {
                    "host": dbfile["host"],
                    "user": dbfile["user"],
                    "password": dbfile["password"],
                    "database": dbfile["database"],
                                }
conn = mysql.connector.connect(**db_config)
    try:
        df = pd.read_csv(Archivo)
        df = df.fillna('')
        cursor = conn.cursor()
        table_name = "grammy"
        for index, row in df.iterrows():
            query = f"INSERT INTO {table_name} (year, title, published_at, updated_at, category, nominee, artist, winner) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(query, (row["year"], row["title"], row["published_at"], row["updated_at"], row["category"], row["nominee"], row["artist"], row["winner"]))
        conn.commit()
        #conn.close()
        print("Data loaded successfully.")

    except Exception as e:
                print("Error:", str(e))


def data_db():
    data = "select * from grammy"
    cursor = conn.cursor()
    cursor.execute(data)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows)
    print(df)
    #conn.close()
    return df

def create_Newtable():
    table = """
        CREATE TABLE IF NOT EXISTS tracks(
            id int NOT NULL AUTO_INCREMENT,
            artists VARCHAR(255),
            album_name VARCHAR(255),
            track_name VARCHAR(255),
            popularity INT,
            explicit BOOLEAN,
            danceability VARCHAR(255),
            energy FLOAT,
            speechiness FLOAT,
            instrumentalness FLOAT,
            tempo INT,
            track_genre VARCHAR(255),
            duration_mmss VARCHAR(255),
            year VARCHAR(255),
            title VARCHAR(255),
            category VARCHAR(255),
            nominee BOOLEAN
        );
    """
    cursor = conn.cursor()
    cursor.execute(table)
    conn.commit()
    print("Tabla creada")

def load_final(df):
    create_Newtable()
    cursor = conn.cursor()
    for _, row in df.iterrows():
        load = f"INSERT INTO tracks (artists, album_name, track_name, popularity, explicit, danceability, energy, speechiness, instrumentalness, tempo, track_genre, duration_mmss, year, title, category, nominee) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(load, (row["artists"], row["album_name"], row["track_name"], row["popularity"], row["explicit"], row["danceability"], row["energy"], row["speechiness"], row["instrumentalness"], row["tempo"], row["track_genre"], row["duration_mmss"], row["year"], row["title"], row["category"], row["nominee"]))
    conn.commit()
    print("Data cargada")
    conn.close()


