# AWS Spark Data Lake

Using Spark to process and transform data into fact & dimension tables in a data lake hosted on S3.

## Description

This project builds an ETL pipeline to construct dimension tables to enable analytics on user activity for the music streaming startup, Sparkify. We design an ETL pipeline to pull in user log data and song metadata from JSON and load into parquet tables in our data lake on S3.

### Schema design

1. **songplays** - Fact table containing song play info. 
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    - For songplay_id, we used the monotonically_increasing_id() function to ensure unique keys across the dataset.
    - The data are partitioned by year and month.
2. **users** - Dimension table with user information
    - user_id, first_name, last_name, gender, level
3. **songs** - Dimension table with info about each song
    - song_id, title, artist_id, year, duration
    - The data are partitioned by year and artist
4. **artists** - Dimension table, one record per artist
    - artist_id, name, location, latitude, longitude
5. **time** - Dimension table with easily accessible info about each songplay start time
    - start_time, hour, day, week, month, year, weekday
    - Partitioned by year and month

### Executing the project

* Run `etl.py` to load raw JSON data. If using spark, the below syntax will work:

```
spark-submit etl.py
```
