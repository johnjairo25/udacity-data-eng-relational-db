import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Imports data into the songs and artists tables based on the information of the JSON file specified by the filepath.
    :param cur:
        Cursor to execute statements against the sparkifyDB
    :param filepath: string
        File path to one of the JSON song files
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    cur.execute(song_table_insert, song_data.to_numpy()[0])
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    cur.execute(artist_table_insert, artist_data.to_numpy()[0])


def process_log_file(cur, filepath):
    """
    Imports data into the users, time and songplays tables based on the information of the JSON file specified by the
    file path.
    :param cur:
        Cursor to execute statements against the sparkifyDB
    :param filepath: string
        File path to one of the JSON events log files
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_dict = {'start_time': df['ts'],
                 'hour': t.dt.hour,
                 'day': t.dt.day,
                 'week': t.dt.week,
                 'month': t.dt.month,
                 'year': t.dt.year,
                 'weekday': t.dt.weekday
                 }
    time_df = pd.DataFrame(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    unique_users_df = user_df[user_df.duplicated() == False]

    # insert user records
    for i, row in unique_users_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row['ts'], row['userId'], row['level'], songid, artistid,
                     row['sessionId'], row['location'], row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Calls the `func` function for every JSON file contained in the folder specified by filepath.
    :param cur:
        Cursor to execute statements against the sparkifyDB database.
    :param conn:
        Connection to the sparkifyDB database.
    :param filepath:
        Folder where files are going to be searched.
    :param func:
        Function to be called for each file found in the folder specified by the `filepath`.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Executes the ETL process to import data into the sparkifydb database.
    Reads all JSON files for songs and log events. And with this information fills in the songs, artists,
    time, users and songplays tables.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()