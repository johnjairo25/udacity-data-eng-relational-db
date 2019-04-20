# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
songplay_id serial PRIMARY KEY,
start_time bigint,
user_id varchar,
level varchar,
song_id varchar,
artist_id varchar,
session_id bigint,
location varchar,
user_agent varchar,
foreign key (start_time) references time(start_time),
foreign key (user_id) references users(user_id),
foreign key (song_id) references songs(song_id),
foreign key (artist_id) references artists(artist_id)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id varchar PRIMARY KEY,
first_name varchar,
last_name varchar,
gender varchar,
level varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
song_id varchar PRIMARY KEY,
title varchar,
artist_id varchar,
duration float,
year int,
foreign key (artist_id) references artists(artist_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
artist_id varchar PRIMARY KEY,
name varchar,
location varchar,
latitude float,
longitude float
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
start_time bigint PRIMARY KEY,
hour int,
day int,
week int,
month int, 
year int,
weekday int
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
insert into songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
values(%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
insert into users(user_id, first_name, last_name, gender, level) 
values(%s, %s, %s, %s, %s)
on conflict(user_id) do nothing
""")

song_table_insert = ("""
insert into songs(song_id, title, artist_id, year, duration) values(%s, %s, %s, %s, %s)
on conflict(song_id) do nothing
""")

artist_table_insert = ("""
insert into artists(artist_id, name, location, latitude, longitude) values(%s, %s, %s, %s, %s)
on conflict(artist_id) do nothing
""")


time_table_insert = ("""
insert into time(start_time, hour, day, week, month, year, weekday) 
values(%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
select songs.song_id, artists.artist_id
from songs join artists on (songs.artist_id = artists.artist_id)
where songs.title ilike %s and artists.name ilike %s and songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]