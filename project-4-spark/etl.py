import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, types
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col, last, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','KEY')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','SECRET')
S3_BUCKET=config.get('AWS','S3_BUCKET')

def create_spark_session():
    """Create and return the SparkSession object
    Parameters: None
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com") \
        .getOrCreate()
    
    return spark


def process_song_data(spark, input_data, output_data):
    """Process the song data. Create a temp view and write to artists parquet files

    Parameters: 
     - spark
     - input_data: path to the song input data in json
     - output_data: path to the output folder
    
    """
    # define schema for song data input
    song_schema = StructType([
        StructField("artist_id", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("year", IntegerType(), True)
    ])

    # read song data file
    df_song_data = spark.read \
        .schema(song_schema) \
        .json(input_data)
    
    # create temp view for use later
    df_song_data.createOrReplaceTempView("songs_data")

    # extract columns to create songs table
    songs_table = df_song_data.select([ \
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration"
    ])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
        .partitionBy("year", "artist_id") \
        .mode("overwrite") \
        .parquet(output_data + "songs.parquet")

    # extract columns to create artists table
    artists_table = df_song_data.selectExpr([ \
        "artist_id", \
        "artist_name as name", \
        "artist_location as location", \
        "artist_latitude as latitude", \
        "artist_longitude as longitude" \
    ]).distinct()
    
    # write artists table to parquet files
    artists_table.write \
        .mode("overwrite") \
        .parquet(output_data + "artists.parquet")

def process_log_data(spark, input_data, output_data):
    """Process the log data. Write to users, time, songplays tables

    Parameters: 
     - spark
     - input_data: path to the song input data in json
     - output_data: path to the output folder
    
    """
    # explicitly define schema for log data input
    log_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", LongType(), True),
        StructField("sessionId", LongType(), True),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("ts", LongType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True)
    ])

    # read log data file
    df_log = spark.read \
        .schema(log_schema) \
        .json(input_data)
    
    # filter by actions for song plays
    df_log_filtered = df_log.filter(df_log.page == "NextSong")

    # create temp view
    df_log_filtered.createOrReplaceTempView('log_data')

    # extract columns for users table    
    users_table = df_log_filtered \
        .groupBy('userId') \
        .agg( 
            last('firstName').alias('first_name'),
            last('lastName').alias('last_name'),
            last('gender').alias('gender'),
            last('level').alias('level')
        ).withColumnRenamed("userId", "user_id")
    
    # write users table to parquet files
    users_table.write \
        .mode("overwrite") \
        .parquet(output_data + "users.parquet")

    # register get_timestamp udf for convering ts column
    spark.udf.register('get_timestamp', lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    
    # extract columns to create time table
    time_table = spark.sql('''
        SELECT  DISTINCT
                ts,
                get_timestamp(ts) as start_time,
                hour(get_timestamp(ts)) as hour,
                dayofmonth(get_timestamp(ts)) as day,
                weekofyear(get_timestamp(ts)) as week,
                month(get_timestamp(ts)) as month,
                year(get_timestamp(ts)) as year,
                date_format(get_timestamp(ts), "E") as weekday
            FROM log_data
    ''')
    time_table.createOrReplaceTempView("time_data")
    
    # write time table to parquet files partitioned by year and month - omit ts field
    time_table.select("start_time", "hour", "day", "week", "month", "year", "weekday") \
        .write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet(output_data + "time.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
        SELECT  monotonically_increasing_id() as songplay_id,
                get_timestamp(l.ts) as start_time,
                l.userId as user_id,
                l.level,
                s.song_id,
                s.artist_id,
                l.sessionId as session_id,
                l.location,
                l.userAgent as user_agent,
                t.year,
                t.month
            FROM log_data l 
            INNER JOIN songs_data s 
                ON l.artist = s.artist_name
                AND l.song = s.title
            INNER JOIN time_data t
                ON t.ts = l.ts
    ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet(output_data + "songplays.parquet")


def main():
    """ The main program
    
    Define input/output variables, and process song & log data.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://" + S3_BUCKET + "/analytics/"
    
    process_song_data(spark, input_data + "song_data/*/*/*/*json", output_data)    
    process_log_data(spark, input_data + "log_data/*/*/*json", output_data)
    
if __name__ == "__main__":
    main()
