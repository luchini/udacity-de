import configparser

# CONSTANTS

S3_SONG_PATH = "s3://udacity-dend/song_data"
S3_LOG_PATH = "s3://udacity-dend/log_data"
S3_LOG_JSON = "s3://udacity-dend/log_json_path.json"

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_IAM_ROLE_NAME      = config.get("DWH","DWH_IAM_ROLE_NAME")
DWH_ROLE_ARN           = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events ;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs ;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays ;"
user_table_drop = "DROP TABLE IF EXISTS users ;"
song_table_drop = "DROP TABLE IF EXISTS songs ;"
artist_table_drop = "DROP TABLE IF EXISTS artists ;"
time_table_drop = "DROP TABLE IF EXISTS time ;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    eventId         BIGINT IDENTITY(0,1),
    artist          VARCHAR(256) NULL,
    auth            VARCHAR(256) NULL,
    firstName       VARCHAR(256) NULL,
    gender          CHAR(1) NULL,
    itemInSession   BIGINT NOT NULL,
    lastName        VARCHAR(256) NULL,
    length          DOUBLE PRECISION NULL,
    level           VARCHAR(256) NULL,
    location        VARCHAR(256) NULL,
    method          VARCHAR(256) NULL,
    page            VARCHAR(256) NULL,
    registration    BIGINT NULL,
    sessionId       BIGINT NULL,
    song            VARCHAR(256) NULL,
    status          BIGINT NULL,
    ts              BIGINT NULL,
    userAgent       VARCHAR(256) NULL,
    userId          BIGINT NULL
) ;
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    artist_id           VARCHAR(256) NULL,
    artist_latitude     DOUBLE PRECISION NULL,
    artist_longitude    DOUBLE PRECISION NULL,
    artist_location     VARCHAR(256) NULL,
    artist_name         VARCHAR(256) NULL,
    song_id             VARCHAR(256) NULL,
    title               VARCHAR(256) NULL,
    duration            DOUBLE PRECISION NULL,
    year                INT NULL
) ;
""")

songplay_table_create = ("""
CREATE TABLE songplays ( 
    songplay_id     BIGINT IDENTITY(0,1),
    start_time      TIMESTAMP NOT NULL,
    user_id         BIGINT NOT NULL,
    level           VARCHAR(256) NOT NULL,
    song_id         VARCHAR(256) NOT NULL,
    artist_id       VARCHAR(256) NOT NULL,
    session_id      INT NOT NULL,
    location        VARCHAR(256) NOT NULL,
    user_agent      VARCHAR(256) NOT NULL
) ;
""")

user_table_create = ("""
CREATE TABLE users (
    user_id     BIGINT PRIMARY KEY,
    first_name  VARCHAR(256) NOT NULL,
    last_name   VARCHAR(256) NOT NULL,
    gender      CHAR(1) NOT NULL,
    level       VARCHAR(256) NOT NULL
) ;
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id     CHAR(18) PRIMARY KEY,
    title       VARCHAR(256) NOT NULL,
    artist_id   VARCHAR(256) NOT NULL,
    year        INT NOT NULL,
    duration    DOUBLE PRECISION NOT NULL
) ;
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id   CHAR(18) PRIMARY KEY,
    name        VARCHAR(256) NOT NULL,
    location    VARCHAR(256) NULL,
    latitude    DOUBLE PRECISION NULL,
    longitude   DOUBLE PRECISION NULL
) ;
""")

time_table_create = ("""
CREATE TABLE time (
    start_time  timestamp PRIMARY KEY,
    hour        INT NOT NULL,
    day         INT NOT NULL,
    week        INT NOT NULL,
    month       INT NOT NULL,
    year        INT NOT NULL,
    weekday     INT NOT NULL
) ;
""")

# STAGING TABLES
staging_events_copy = ("""
COPY staging_events
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    JSON '{}' ;
""").format(S3_LOG_PATH, DWH_ROLE_ARN, S3_LOG_JSON)

staging_songs_copy = ("""
COPY staging_songs
    FROM '{}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto' TRUNCATECOLUMNS ;
""").format(S3_SONG_PATH, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT  DISTINCT
        (TIMESTAMP 'epoch' + se.ts/1000 * interval '1 Second'),
        se.userId,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
    FROM staging_events se
    INNER JOIN staging_songs ss
        ON ss.artist_name = se.artist
        AND ss.title = se.song
        AND ss.duration = se.length
    WHERE se.page = 'NextSong' ;
""")

#
# Use a correlated subquery to ensure the most up-to-date info for each user
#
user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT  se.userId,
        se.firstName,
        se.lastName,
        se.gender,
        se.level
    FROM staging_events se
    WHERE se.eventId = (
        SELECT  se0.eventId
            FROM staging_events se0
            WHERE se0.userId = se.userId
            ORDER BY se0.ts DESC
            LIMIT 1
    ) ;
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT  DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs ;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT  DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs ;
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT  DISTINCT
        start_time,
        EXTRACT(hour FROM start_time),
        EXTRACT(day FROM start_time),
        EXTRACT(week FROM start_time),
        EXTRACT(month FROM start_time),
        EXTRACT(year FROM start_time),
        EXTRACT(weekday FROM start_time)
    FROM songplays ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]