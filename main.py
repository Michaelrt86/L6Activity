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
favorite_genres = song_metadata_spark.join(listening_logs_spark, "song_id").groupBy("user_id", "genre").agg(count("*").alias("listen_count"))

#Define a filter that counts the highest genre for each user and only displays the top genre for each user
window_spec = Window.partitionBy("user_id").orderBy(col("listen_count").desc())
favorite_genres = favorite_genres.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1).drop("rank").orderBy(col("listen_count").desc())
favorite_genres.show(10)

# Task 2: Average Listen Time
avg_listen_per_user = listening_logs_spark.groupBy("user_id").agg(avg("duration_sec").alias("avg_listen_time_sec"))
avg_listen_per_user.show(10)


# Task 3: Create your own Genre Loyalty Scores and rank them and list out top 10
genre_loyalty_scores = song_metadata_spark.join(listening_logs_spark, "song_id").groupBy("user_id", "genre").agg(count("*").alias("listen_count"))
genre_loyalty_scores = genre_loyalty_scores.withColumn("total_listens", sum("listen_count").over(Window.partitionBy("user_id"))).withColumn("loyalty_score", col("listen_count") / col("total_listens"))
genre_loyalty_scores = genre_loyalty_scores.withColumn("rank", row_number().over(Window.partitionBy("user_id").orderBy(col("loyalty_score").desc()))).filter(col("rank") == 1).drop("rank").orderBy(col("loyalty_score").desc())
genre_loyalty_scores.show(10)


# Task 4: Identify users who listen between 12 AM and 5 AM
night_owls = listening_logs_spark.withColumn("hour", hour(col("timestamp"))).filter((col("hour") >= 0) & (col("hour") < 5)).select("user_id").distinct()
night_owls.show(10)


#Output results
favorite_genres.write.csv("outputs/user_song_counts", header=True, mode="overwrite")
avg_listen_per_user.write.csv("outputs/avg_listen_per_user", header=True, mode="overwrite")

#Task 4 Completed
night_owls.write.csv("outputs/night_owls", header=True, mode="overwrite")

spark.stop()