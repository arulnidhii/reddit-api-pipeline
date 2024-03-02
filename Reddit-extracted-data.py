# Databricks notebook source
import json
pip install praw
pip install azure-storage-blob

from pyspark.sql import SparkSession
from praw import Reddit
from azure.storage.blob import BlobServiceClient, BlobClient

client_id = 'xxxx'
client_secret = 'xxxx'
user_agent = 'my_reddit_scraper'
connection_string = 'DefaultEndpointsProtocol=https;AccountName=redditsaarul123;AccountKey=xxxx;EndpointSuffix=core.windows.net'
container_name = 'reddit-Data-Extracted'

# Authenticating Reddit API
reddit = Reddit(client_id=client_id,
                 client_secret=client_secret,
                 user_agent=user_agent)

# Specifying the subreddit
subreddit_name = 'dataengineering'
subreddit = reddit.subreddit(subreddit_name)

cluster_url = "https://xxxx.azuredatabricks.net/xxxx#setting/clusters/xxxx/sparkClusterUi"

# Create SparkSession

spark = SparkSession.builder.appName("RedditDataPipeline") \
    .config("spark.master", cluster_url) \
    .getOrCreate()

def process_post_data(post):
    data = {
        "id": post.id,
        "title": post.title,
        "author": post.author.name,
        "selftext": post.selftext,
        "created_utc": post.created_utc,
        "word_count": len(post.selftext.split())
    }
    return data

# Create an empty DataFrame with defined schema
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

# Schema Definition
schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("created_utc", LongType(), True),
    StructField("word_count", IntegerType(), True)
])

# Create an empty DataFrame with the defined schema
data_df = spark.createDataFrame([], schema=schema)

# Retrieve posts and build DataFrame
for post in subreddit.hot(limit=100):
    processed_data = process_post_data(post)
    processed_data["created_utc"] = int(processed_data["created_utc"])  # Convert to long
    # Append data to DataFrame
    data_df = data_df.union(spark.createDataFrame([processed_data], schema=data_df.schema))

data_df.show()

from pyspark.sql import functions as F

data_df = data_df.withColumn("created_date", F.to_date(data_df["created_utc"], "yyyy-MM-dd"))


data_df.count()

dbfs_path = f"dbfs:/data/reddit"

data_df.write \
    .format("csv") \
    .option("header", True) \
    .save(dbfs_path)


