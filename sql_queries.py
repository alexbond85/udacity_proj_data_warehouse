import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES
drop_table = lambda x: f"DROP TABLE IF EXISTS {x}"
staging_events_table_drop = drop_table("staging_events")
staging_songs_table_drop = drop_table("staging_songs")
songplay_table_drop = drop_table("songplays")
user_table_drop = drop_table("users")
song_table_drop = drop_table("songs")
artist_table_drop = drop_table("artists")
time_table_drop = drop_table("time")

# CREATE TABLES

# Staging tables

# sort by sessionId and distribute by sessionId
# introduce an auto incrementing ID column
staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist      VARCHAR                 NULL,
        auth        VARCHAR                 NULL,
        firstName   VARCHAR                 NULL,
        gender      VARCHAR                 NULL,
        itemInSession VARCHAR               NULL,
        lastName    VARCHAR                 NULL,
        length      VARCHAR                 NULL,
        level       VARCHAR                 NULL,
        location    VARCHAR                 NULL,
        method      VARCHAR                 NULL,
        page        VARCHAR                 NULL,
        registration VARCHAR                NULL,
        sessionId   INTEGER                 NOT NULL SORTKEY DISTKEY,  
        song        VARCHAR                 NULL,
        status      INTEGER                 NULL,
        ts          BIGINT                  NOT NULL,
        userAgent   VARCHAR                 NULL,
        userId      INTEGER                 NULL,
        staging_events_id    BIGINT IDENTITY(0,1)    NOT NULL);  
""")

# sort by artist_id, distribute also by artist_id
staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
                num_songs           INTEGER         NULL,
                artist_id           VARCHAR         NOT NULL SORTKEY DISTKEY,  
                artist_latitude     VARCHAR         NULL,
                artist_longitude    VARCHAR         NULL,
                artist_location     VARCHAR         NULL,
                artist_name         VARCHAR         NULL,
                song_id             VARCHAR         NOT NULL,
                title               VARCHAR         NULL,
                duration            DECIMAL(9)      NULL,
                year                INTEGER         NULL
    );
""")

# fact tables

# distribute by user_id
# introduce an auto incrementing ID column songplay_id
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
                songplay_id INTEGER IDENTITY(0,1)    NOT NULL SORTKEY,
                start_time  TIMESTAMP                NOT NULL,
                user_id     VARCHAR(100)             NOT NULL DISTKEY,  
                level       VARCHAR(100)              NOT NULL,
                song_id     VARCHAR(100)              NOT NULL,
                artist_id   VARCHAR(100)              NOT NULL,
                session_id  VARCHAR(100)              NOT NULL,
                location    VARCHAR(100)              NULL,
                user_agent  VARCHAR(500)              NULL
    );
""")

# dimension tables

# broadcast
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
                user_id     VARCHAR(100)              NOT NULL SORTKEY,
                first_name  VARCHAR(100)              NULL,
                last_name   VARCHAR(100)              NULL,
                gender      VARCHAR(30)               NULL,
                level       VARCHAR(100)              NULL
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
                song_id     VARCHAR(100)             NOT NULL SORTKEY,
                title       VARCHAR(500)             NOT NULL,
                artist_id   VARCHAR(100)             NOT NULL,
                year        INTEGER                  NOT NULL,
                duration    DECIMAL(9)               NOT NULL
    );
""")

# broadcast
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
                artist_id   VARCHAR(100)             NOT NULL SORTKEY,
                name        VARCHAR(500)             NULL,
                location    VARCHAR(500)             NULL,
                latitude    DECIMAL(9)               NULL,
                longitude   DECIMAL(9)               NULL
    ) diststyle all;
""")

# broadcast
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
                start_time  TIMESTAMP               NOT NULL SORTKEY,
                hour        SMALLINT                NULL,
                day         SMALLINT                NULL,
                week        SMALLINT                NULL,
                month       SMALLINT                NULL,
                year        SMALLINT                NULL,
                weekday     SMALLINT                NULL
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id,
                           artist_id, session_id, location, user_agent)
    SELECT  DISTINCT 
          TIMESTAMP 'epoch' + (se.ts/1000) * INTERVAL '1 second' AS start_time,
          se.userId                                              AS user_id,
          se.level                                               AS level,
          ss.song_id                                             AS song_id,
          ss.artist_id                                           AS artist_id,
          se.sessionId                                           AS session_id,
          se.location                                            AS location,
          se.userAgent                                           AS user_agent
    FROM staging_events AS se
    INNER JOIN staging_songs AS ss
        ON (se.artist = ss.artist_name AND se.song = ss.title)
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT se.userId                   AS user_id,
                     se.firstName                AS first_name,
                     se.lastName                 AS last_name,
                     se.gender                   AS gender,
                     se.level                    AS level
    FROM staging_events AS se
    WHERE se.page = 'NextSong' AND se.userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT ss.song_id         AS song_id,
            ss.title                    AS title,
            ss.artist_id                AS artist_id,
            ss.year                     AS year,
            ss.duration                 AS duration
    FROM staging_songs AS ss
    WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (               artist_id,
                                        name,
                                        location,
                                        latitude,
                                        longitude)
    SELECT  DISTINCT ss.artist_id       AS artist_id,
            ss.artist_name              AS name,
            ss.artist_location          AS location,
            ss.artist_latitude          AS latitude,
            ss.artist_longitude         AS longitude
    FROM staging_songs AS ss
    WHERE ss.artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (                  start_time,
                                        hour,
                                        day,
                                        week,
                                        month,
                                        year,
                                        weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_events                   AS se
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
