from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year, 
    month, 
    dayofmonth, 
    hour, 
    weekofyear, 
    dayofweek, 
    udf, 
    col,
    row_number, 
    monotonically_increasing_id
)

from pyspark.sql import types as T
from pyspark.sql import Window
from datetime import datetime


def create_spark_session():
    spark = SparkSession.builder.getOrCreate()
    return spark


"""
    This procedure processes song files whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into songs table and then stores it in delta format.
    It also extracts the artist information in order to store it into artists table and then stores it in delta format.

    INPUTS:
    * spark the spark connection variable
    * input_data the s3 bucket path to the song data
    * output_data the s3 bucket path to store songs and artists table
"""
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song-data/*/"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'artist_name', 'year', 'duration'])
    
    # write songs table to delta files partitioned by year and artist
    songs_table.write.mode('overwrite').format("delta").save(output_data+'songs')

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
    
    # write artists table to delta files
    artists_table.write.mode('overwrite').format("delta").save(output_data + 'artists')


"""
    This procedure processes log files whose filepath has been provided as an arugment.
    It extracts the song start time information, tansforms it and then store it into the time table in delta format.
    Then it extracts the users information in order to store it into the users table in delta format.
    Finally it extrats informations from songs table and original log file to store it into the songplays table in delta format.

    INPUTS:
    * spark the spark connection variable
    * input_data the s3 bucket path to the log data
    * output_data the s3 bucket path to store users, time and song_plays table
"""    
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log-data"

    # read log data file
    #df = spark.read.json(log_data + "/2018/11/2018-11-*.json")
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level'])
    
    # write users table to delta files
    users_table.write.mode('overwrite').format("delta").save(output_data + 'users')

    # create timestamp column from original timestamp column    
    get_timestamp = udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0).strftime("%Y-%m-%d %H:%M:%S"))
    df = df.withColumn("start_time", get_datetime(df.ts))
    
    df = df.withColumn("hour", hour(col("start_time")))\
           .withColumn("day", dayofmonth(col("start_time")))\
           .withColumn("week", weekofyear(col("start_time")))\
           .withColumn("month", month(col("start_time")))\
           .withColumn("year", year(col("start_time")))\
           .withColumn("weekday", dayofweek(col("start_time")))
        
    # extract columns to create time table
    time_table = df.select(['timestamp', 'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'])
    
    # write time table to delta files partitioned by year and month
    time_table.write.mode('overwrite').format("delta").save(output_data+'time')

    # read in song data to use for songplays table
    song_df = spark.read.format("delta").load(output_data + '/songs/')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table=song_df.select(['song_id', 'title', 'artist_id', 'artist_name', 'duration']) \
                           .join(df, (song_df.artist_name == df.artist) \
                                     & (song_df.title == df.song)) \
                           .select(['ts', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent', 'year', 'month'])
    
    songplays_table = songplays_table.withColumn(
                      "songplay_id",
                      row_number().over(Window.orderBy(monotonically_increasing_id()))-1
    )
    
    # write songplays table to delta files partitioned by year and month
    songplays_table.write.mode('overwrite').format("delta").save(output_data + 'songplays')


def main():
    spark = create_spark_session()
    input_data = "s3://owshq-landing-zone-777696598735/"
    output_data = "s3://owshq-bronze-777696598735/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()