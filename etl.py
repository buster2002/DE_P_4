import configparser
from datetime import datetime
import calendar
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data,input_song_data, output_data):
    # get filepath to song data file
    print("Starting ...")
    song_data = os.path.join(input_data, input_song_data)

    # read song data file
    print("Reading Song Data from File")
    df = spark.read.json(song_data)
    print("Done, Read")

    # extract columns to create songs table
    #•	song_id, title, artist_id, year, duration
    print("Extracting columns to create songs table")
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    print("Done, Extracted")

    # write songs table to parquet files partitioned by year and artist
    print("Writing songs table to parquet files partitioned by year and artist")
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')
    print("Done, Wrote")

    # extract columns to create artists table
    #•	artist_id, name, location, lattitude, longitude
    print("Extracting columns to create artists table")
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    print("Done, Extracted")

    # write artists table to parquet files
    print("Writing artists table to parquet files")
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')
    print("Done, Wrote")

def process_log_data(spark, input_data, input_log_data, output_data):
    # get filepath to log data file
    print("Starting ...")
    log_data = os.path.join(input_data, input_log_data)

    # read log data file
    print("Reading Log Data from File")
    df = spark.read.json(log_data)
    print("Done, Read")

    # filter by actions for song plays
    print("filtering by actions for song plays")
    df = df.filter(F.col("page") == "NextSong")
    print("Done, filtered")

    # extract columns for users table
    # •	user_id, first_name, last_name, gender, level
    # given input columns: [itemInSession, lastName, auth, sessionId, firstName, userId, location, registration, gender, status, level, artist, ts, userAgent, page, length, song, method]
    print("Extracting columns to create users table")
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    print("Done, Extracted")

    # write users table to parquet files
    print("Writing users table to parquet files")
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')
    print("Done, Wrote")

    # create timestamp column from original timestamp column
#    get_timestamp = udf()
#    df =

    # create datetime column from original timestamp column
#    get_datetime = udf()
#    df =

    # extract columns to create time table
#    time_table =

    # write time table to parquet files partitioned by year and month
#    time_table

    # read in song data to use for songplays table
#    song_df =

    # extract columns from joined song and log datasets to create songplays table
#    songplays_table =

    # write songplays table to parquet files partitioned by year and month
#    songplays_table


def main():
    spark = create_spark_session()

    input_data = "s3a://udacity-dend/"

    output_data = "s3a://buraik-udacity/DE"

#    input_song_data = "song_data/*/*/*/*.json"
    input_song_data = "song_data/A/B/C/TRABCEI128F424C983.json"
    
#    input_log_data = "log_data/*/*/*.json"
    input_log_data = "log_data/2018/11/2018-11-12-events.json"

#    process_song_data(spark, input_data, input_song_data, output_data)
    process_log_data(spark, input_data, input_log_data, output_data)


if __name__ == "__main__":
    main()
