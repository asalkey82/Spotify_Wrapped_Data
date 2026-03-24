import os
import sqlite3
import json
from tqdm import tqdm
from pathlib import Path
import pandas as pd
import pprint
import requests
from collections import Counter

def populate_db(conn, cur, music_path):
    # Check if table already exists; drop if it does
    cur.execute("DROP TABLE IF EXISTS music_data")
    # Open schema file to create table
    with open('schema.sql', 'r') as sql_file:
        sql_script = sql_file.read()
    # Execute script and
    cur.executescript(sql_script)
    conn.commit()
    # List all data files in path
    for name in os.listdir(music_path):
        with open(os.path.join(music_path, name), encoding='utf-8') as f:
            streaming_data = json.load(f)
            # Open and loop through the json
            # tqdm display progress bar in terminal
            for item in tqdm(streaming_data):
                # If track name is null, entry is a podcast
                # Only gathering music data
                if item['master_metadata_track_name'] is not None:
                    # Reformatting timestamp into a readable format
                    date_str = item["ts"].replace("T", ' ').replace("Z", '')
                    # Insert data into table
                    cur.execute("INSERT INTO music_data(song, album, artist, date, time_listened, reason_end, song_id) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                                (
                                    item["master_metadata_track_name"], item["master_metadata_album_album_name"],
                                    item["master_metadata_album_artist_name"], date_str, item["ms_played"],
                                    item["reason_end"], item["spotify_track_uri"]
                                )
                               )
                conn.commit()


def unique_entries(df, columns):
    """
    Create data frame with single instance of song stats
    Used to pull data about song entry
    """
    columns = ['date', 'reason_end', 'id']
    return df.drop_duplicates().drop(columns, axis=1)


def total_time(df):
    """
    Find the total amount of minutes listened to in a year
    """
    total_ms = df['time_listened'].sum()
    total_mins = (total_ms/60000)

    return int(total_mins)


def top_listen_month(df):
    top_month = 0
    total = 0

    for month in range(12):
        month_total = df[df["date"].dt.month == month]
        month_sum = total_time(month_total)
        if month_sum >= total:
            total = month_sum
            top_month = month
    
    return top_month

def top_ten(df, column):
    """
    Finds the top five frequent value withing a column
    """
    times_played = df[column].value_counts().reset_index(name='Frequency')
    top = times_played[column].iloc[0:10].to_list()

    return top

def get_info(songs: list):
    #  {"song": "", "track_preview": "", "song_art": ""}
    info_dict = {}
    genre = []
    
    for song in songs:
        response = requests.get("itunes.apple.com/search?term" + song + "&entity=song&limit=1")
        rtn = response.json()
        data = rtn["results"][0]

        info_dict.update({"artist": data.artistName})
        info_dict.update({"art_work": data.artworkUrl100})
        info_dict.update({"album": data.collectionName})
        info_dict.update({"preview": data.previewUrl})

        genre.append(data.primaryGenreName)

    count_genre = Counter(genre)
    top_genre = count_genre.most_common(1)

    return top_genre


def main():
    music_path = Path("C:\\Users\\aesal\\OneDrive\\Documents\\scripts\\sql_test\\spotify_stats/2023")

    conn = sqlite3.connect('db.db')
    cur = conn.cursor()

    populate_db(conn, cur, music_path)

    wrapped_data = {"top_artists": [], "top_albums": [], "top_songs": [], "total_time": 0, "genre": ""}

    query = "SELECT * FROM music_data;"

    df = pd.read_sql_query(query, conn)
    df["date"] = pd.to_datetime(df['date'])

    year = 2023

    year_data = df[df["date"].dt.year == year]
    filtered_songs = year_data[year_data['reason_end'] == 'trackdone']

    top_songs = top_ten(filtered_songs, 'song')
    top_artists = top_ten(filtered_songs, 'artist')
    top_albums = top_ten(filtered_songs, 'album')
    time_in_mins = total_time(year_data)

    wrapped_data["top_artists"] = top_artists
    wrapped_data["top_albums"] = top_albums
    wrapped_data["top_songs"] = top_songs
    wrapped_data["total_time"] = time_in_mins

    print(f"Spotify data for {year}")
    pprint.pprint(wrapped_data)
    print("\n")

    conn.close()


if __name__ == '__main__':
  main()