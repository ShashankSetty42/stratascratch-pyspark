#You're tasked with analyzing a Spotify-like dataset that captures user listening habits.
#
#For each user, calculate the total listening time and the count of unique songs they've listened to. In the database duration values are displayed in seconds. Round the total listening duration to the nearest whole minute.
#
#The output should contain three columns: 'user_id', 'total_listen_duration', and 'unique_song_count'.

# Import your libraries
import pyspark
from pyspark.sql import functions as f

# Start writing code
listening_habits = listening_habits.groupBy("user_id").agg(f.sum("listen_duration").alias('total_duration'), f.countDistinct("song_id").alias('distinct_songs')).select("user_id", "total_duration","distinct_songs")

# To validate your solution, convert your final pySpark df to a pandas df
listening_habits.toPandas()