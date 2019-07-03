import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import TimestampType


def create_spark_session():
    """
    func: This function creates a spark sql session for data
    transformation and manipulation
    motivation: This function particularly configures an environment to
    read and write from Amazon AWS
    :return:
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    func: This function takes 3 input, reads from an s3 bucket and writes
    in parquet format to another s3 bucket with the help of apache
    spark.
    motivation: This function reads metadata files in json format to
    create songs and artists table
    :param spark: spark session for data transformation and warehousing
    :param input_data: the input directory
    :param output_data: the write directory
    :return:
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    # set output for songs table
    output_data_songs = os.path.join(output_data, "songs")

    # set output for artists table
    output_data_artists = os.path.join(output_data, "artists")

    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_df.selectExpr("song_id",
                                     "title",
                                     "artist_id",
                                     "cast(year as int)",
                                     "duration")\
                         .dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data_songs,
                              mode='overwrite',
                              partitionBy=["year", "artist_id"])

    # extract columns to create artists table
    artists_table = song_df.selectExpr("artist_id",
                                       "artist_name as name",
                                       "coalesce(nullif(artist_location, ''), 'N/A') as location",
                                       "coalesce(artist_latitude, 0.0) as latitude",
                                       "coalesce(artist_longitude, 0.0) as longitude")\
                           .dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data_artists, mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    func: This function takes 3 input, reads from an s3 bucket and writes
    in parquet format to another s3 bucket with the help of apache
    spark.
    motivation: This function reads metadata files in json format to
    create songplays, users and time table
    :param spark: spark session for data transformation and warehousing
    :param input_data: the input directory
    :param output_data: the write directory
    :return:
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*.json")

    # set output path for time table
    output_data_time = os.path.join(output_data, "time")

    # set output path for user table
    output_data_user = os.path.join(output_data, "user")

    # set output path for songplays table
    output_data_songplays = os.path.join(output_data, "songplays")

    # set input for songs table
    input_data_songs = os.path.join(output_data, "songs")

    # set input for artists table
    input_data_artists = os.path.join(output_data, "artists")

    # read log data file
    log_df = spark.read.json(log_data)

    # filter by actions for song plays
    log_df = log_df.drop_duplicates()\
                   .filter(log_df.page=="NextSong")

    # create udf for time table
    @udf(TimestampType())
    def parseDate(col_name):
        col_name_div = col_name / 1000
        col_name_converted = datetime.fromtimestamp(col_name_div)
        return col_name_converted

    # create timestamp column
    log_df = log_df.withColumn("timestamp", parseDate(log_df["ts"]))

    # create time table
    time_table = log_df.selectExpr("timestamp as start_time",
                                "hour(timestamp) as hour",
                                "dayofmonth(timestamp) as day",
                                "weekofyear(timestamp) as week",
                                "month(timestamp) as month",
                                "year(timestamp) as year",
                                "date_format(timestamp,'E') as weekday")

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data_time,
                             mode='overwrite',
                             partitionBy=["year", "month"])

    # create users table
    user_table = log_df.selectExpr("userId as user_id",
                                   "firstName as first_name",
                                   "lastName as last_name",
                                   "gender",
                                   "coalesce(nullif(level, ''), 'N/A')"
                                   )

    # write user table to parquet files
    user_table.write.parquet(output_data_user, mode='overwrite')

    # read song table
    song_df = spark.read.parquet(input_data_songs)

    # read artists table
    artists_df = spark.read.parquet(input_data_artists)

    # create songplays table
    songplays_table = log_df.alias("lg")\
                            .filter(log_df.page == "NextSong")\
                            .join(song_df.alias("sg"),
                                  log_df.song == song_df.title, "leftouter")\
                            .join(artists_df.alias("ar"),
                                  log_df.artist == artists_df.name, "leftouter")\
                            .selectExpr("lg.timestamp as start_time",
                                        "lg.userId as user_id",
                                        "lg.level as level",
                                        "sg.song_id as song_id",
                                        "coalesce(ar.artist_id, sg.artist_id) as artist_id",
                                        "lg.sessionId as session_id",
                                        "ar.location as location",
                                        "lg.userAgent as user_agent")\
                            .dropDuplicates()

    # write songplays table to parquet files partitioned by year and
    # month
    songplays_table.write.parquet(output_data_songplays,
                                  mode='overwrite',
                                  partitionBY=["year", "month"])


def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    # set aws credentials
    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['KEY']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['SECRET']

    # create spark session
    spark = create_spark_session()

    # if the above doesn't work
    # option 2
    access_key = config['AWS']['KEY']
    secret_key = config['AWS']['SECRET']

    spark.sparkContext._jsc.hadoopConfiguration()\
                           .set("fs.s3a.access.key", access_key)
    spark.sparkContext._jsc.hadoopConfiguration()\
                           .set("fs.s3a.secret.key", secret_key)

    input_data = config['S3']['S3_BUCKET_INPUT_PATH']
    output_data = config['S3']['S3_BUCKET_OUTPUT_PATH']
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
