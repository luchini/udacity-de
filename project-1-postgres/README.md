# Data Modeling and ETL for music streaming

Creating a database schema and a batch ETL pipeline for the music streaming app, Sparkify.

## Description

This project designs a database with a star schema data model in PostgreSQL to enable analysis on user activity. We pull in user log data and song metadata - both in JSON - and export to the PostgreSQL tables.

### Schema design

We create the `sparkifydb` database with the following tables, arranged in a star schema around `songplays`:

1. **songplays** - Fact table containing song play info. 
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
2. **users** - Dimension table with user information
    - user_id, first_name, last_name, gender, level
3. **songs** - Dimension table with info about each song
    - song_id, title, artist_id, year, duration
4. **artists** - Dimension table, one record per artist
    - artist_id, name, location, latitude, longitude
5. **time** - Dimension table with easily accessible info about each songplay start time
    - start_time, hour, day, week, month, year, weekday

We enforced each first column as the primary key for each table. For `songplays`, we made `songplay_id` a serial datatype so it would be auto-incremented. All other primary keys were found in the existing data. For our insertion queries, we leveraged the primary key constraints with `DO NOTHING` commands to ensure we never duplicated data.

### Executing the project

* Run `create_tables.py` to drop/create the sparkify db and drop/create empty tables
* Run `etl.py` to parse the JSON user activity and song metadata

```
python create_tables.py
python etl.py
```

### Additional queries

Once the user activity is stored in our relational database, the Sparkify data analytics team can dive in to better understand our user behavior, activity, and target markets. With and end-goal of increasing app usage and increasing active user count. Here are a few simple examples of sample queries:

* When are our users using sparkify? Perhaps unsurprisingly, Friday afternoons and evenings are most popular
```
SELECT  t.weekday,
        t.hour,
        COUNT(*) AS count
    FROM songplays s 
    JOIN time t ON t.start_time = s.start_time 
    GROUP BY
      t.weekday, 
      t.hour 
    ORDER BY count desc 
    LIMIT 10 ;
```

* What are our most popular location markets? Note: it would be important to bring in population data into a location-based dimension table in order to better understand relative popularity.
```
SELECT  location, 
        COUNT(DISTINCT user_id) AS user_count, 
        COUNT(DISTINCT session_id) AS session_count 
    FROM songplays 
    GROUP BY location
    ORDER BY user_count DESC 
    LIMIT 5;
```

* We can find active users that only listen to a few songs per session. This is a group that would be a useful target for A/B testing to increase user engagement through improved song recommendations, UI updates, etc. Note: for the sake of this example, we're an active user as someone with more than 5 listening sessions. 
```
SELECT  plays.user_id, 
        AVG(plays.plays_per_session) AS avg_plays_per_session
  FROM (
    SELECT  user_id, 
            session_id, 
            COUNT(*) AS plays_per_session 
        FROM songplays 
        GROUP BY
            user_id, 
            session_id
  ) plays 
  GROUP BY plays.user_id 
  HAVING COUNT(*) > 5
  ORDER BY avg_plays_per_session ASC
  LIMIT 10 ;
```

* With a bit more song metadata, we could look for trends by artist, genre, and use that information to build song/artist recommendation algorithms.
