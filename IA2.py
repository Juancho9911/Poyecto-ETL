from flask import Flask, render_template, request, redirect, url_for
import psycopg2
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import tensorflow as tf
from tensorflow.keras import layers, models
from sklearn.preprocessing import LabelEncoder

app = Flask(__name__)

def connect_db():
    """Establece una conexión con la base de datos PostgreSQL."""
    try:
        conn = psycopg2.connect(
            database="etl",
            user="postgres",
            password="12345",
            host="localhost",
            port="5432"
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def extract_tracks(conn):
    """Extrae las pistas de la base de datos."""
    try:
        cur = conn.cursor()
        cur.execute('SELECT "TrackId", "Name", "AlbumId", "GenreId" FROM "Track"')
        columns = [desc[0] for desc in cur.description]
        tracks = cur.fetchall()
        df_tracks = pd.DataFrame(tracks, columns=columns)
        return df_tracks
    except psycopg2.Error as e:
        print(f"Error al extraer las pistas: {e}")
        return pd.DataFrame()

def generate_song_embeddings(df):
    """Genera embeddings para las canciones usando características como el género y el álbum."""

    le_genre = LabelEncoder()
    le_album = LabelEncoder()
    
    df['GenreEncoded'] = le_genre.fit_transform(df['GenreId'])
    df['AlbumEncoded'] = le_album.fit_transform(df['AlbumId'])
    
    model = models.Sequential([
        layers.Embedding(input_dim=max(df['GenreEncoded'].max(), df['AlbumEncoded'].max()) + 1, output_dim=8),
        layers.Flatten(),
        layers.Dense(16, activation='relu'),
        layers.Dense(8, activation='relu')  
    ])
    

    features = np.stack([df['GenreEncoded'], df['AlbumEncoded']], axis=1)
    embeddings = model.predict(features)
    
    return embeddings

def generate_playlist(df, num_tracks=10):
    """Genera una playlist usando embeddings de canciones."""
    if df.empty:
        return pd.DataFrame()


    embeddings = generate_song_embeddings(df)
    

    kmeans = KMeans(n_clusters=num_tracks, random_state=0)
    kmeans.fit(embeddings)
    
    playlist = []
    labels = kmeans.labels_

    for cluster in range(num_tracks):
        cluster_tracks = df[labels == cluster]
        if not cluster_tracks.empty:
            track = cluster_tracks.sample(n=1)
            playlist.append(track)

    playlist_df = pd.concat(playlist, ignore_index=True)
    
    return playlist_df

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/generate', methods=['POST'])
def generate():
    conn = connect_db()
    if conn:
        try:
            df_tracks = extract_tracks(conn)
            playlist_df = generate_playlist(df_tracks, num_tracks=10)
            html_table = playlist_df[['TrackId', 'Name']].to_html(index=False)
            conn.close()
            return render_template('playlist.html', table=html_table)
        except Exception as e:
            print(f"Error durante el procesamiento de datos: {e}")
            return str(e)
    else:
        return "No se pudo establecer la conexión con la base de datos."

if __name__ == "__main__":
    app.run(debug=True)
