# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import count, sum as spark_sum
import os

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load datasets
listening_logs_spark = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
song_metadata_spark = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Task 1: User Favorite Genres
#listening_logs_spark = listening_logs_spark.join(song_metadata_spark.select("song_id", "genre"), on="song_id", how="inner")
user_song_counts = listening_logs_spark.groupBy("user_id", "genre").agg(count("*").alias("listen_count"), spark_sum("duration_sec").alias("total_duration"))
favorite_genres = user_song_counts.withColumn("rank", row_number().over(Window.partitionBy("user_id").orderBy(col("listen_count").desc(), col("total_duration").desc()))).filter(col("rank") == 1).select("user_id", "genre", "listen_count", "total_duration")

# Task 2: Average Listen Time
avg_listen_per_user = listening_logs_spark.groupBy("user_id").agg(avg("duration_sec").alias("avg_listen_time_sec"))
avg_listen_per_user.show(10)


# Task 3: Create your own Genre Loyalty Scores and rank them and list out top 10


# Task 4: Identify users who listen between 12 AM and 5 AM
listening_logs_spark = listening_logs_spark.withColumn("hour", hour(col("timestamp")))


#Output results
user_song_counts.write.csv("outputs/user_song_counts", header=True, mode="overwrite")
avg_listen_per_user.write.csv("outputs/avg_listen_per_user", header=True, mode="overwrite")


spark.stop()