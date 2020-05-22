import configparser
from datetime import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import TimestampType, StringType

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


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+'song_data/[A-Z]/[A-Z]/[A-Z]/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').orderBy('song_id').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(output_data+"songs_table.parquet")
    
    # extract columns to create artists table
    artists_table = df.selectExpr(
        'artist_id', 
        'artist_name as name', 
        'artist_location as location', 
        'artist_latitude as lattitude', 
        'artist_longitude as longitude'
    ).orderBy('artist_id').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+"artists_table.parquet")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =input_data+'log-data'
#     log_data = input_data+'log_data/[0-9]*/[0-9]*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')

    # extract columns for users table    
    users_table = users_table = df.selectExpr(
        'cast(userId as long) as user_id', 
        'firstName as first_name', 
        'lastName as last_name', 
        'gender', 
        'level'
    ).orderBy('user_id').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+"users_table.parquet")

    # datetime & weekday UDFs
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0), TimestampType())
    get_weekday = udf(lambda x: 'True' if datetime.fromtimestamp(x/1000.0).weekday in [5,6] else 'False', StringType())
    
    # create datetime column from original timestamp column
    df = df.withColumn('datetime', get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df.selectExpr(
        'ts as start_time', 
        'cast(hour(datetime) as short) as hour', 
        'cast(dayofmonth(datetime) as short) as day', 
        'cast(weekofyear(datetime) as short) as week',
        'cast(month(datetime) as short) as month', 
        'cast(year(datetime) as short) as year'
    ).withColumn('weekday', get_weekday('start_time')).orderBy('start_time').dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data+"time_table.parquet")

    # read in song data to use for songplays table
    song_data = input_data+'song_data/[A-Z]/[A-Z]/[A-Z]/*.json'
    song_df = spark.read.json(song_data)
    
    # join song & log tables on song title, artist name & duration
    round_str = udf(lambda x: int(x))
    df= df.withColumn('length_round', round_str('length'))
    song_df= song_df.withColumn('duration_round', round_str('duration'))

    cond = [song_df.title==df.song, song_df.artist_name==df.artist, song_df.duration_round==df.length_round]
    joined_df = df.join(song_df, cond, how='left_outer')

    # extract columns from joined song and log datasets to create songplays table 
    joined_df = joined_df.withColumn('songplay_id', monotonically_increasing_id())
    songplays_table = joined_df.selectExpr(
        'songplay_id',
        'ts as start_time', 
        'userId as user_id', 
        'level', 
        'song_id', 
        'artist_id', 
        'sessionId as session_id', 
        'location', 
        'userAgent as user_agent',
        'cast(month(datetime) as short) as month', 
        'cast(year(datetime) as short) as year'
    ).orderBy('songplay_id').dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data+"songplays_table.parquet")



def main():
    spark = create_spark_session()

#     input_data = "s3a://udacity-dend/"
#     input_data = "s3a://hariraja-playground/"
#     output_data = "s3a://hariraja-playground/info/"
    
    input_data = "./data/"
    output_data = "./info/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
