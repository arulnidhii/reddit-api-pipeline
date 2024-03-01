# Databricks notebook source
import json

# COMMAND ----------

pip install praw

# COMMAND ----------

pip install azure-storage-blob

# COMMAND ----------

from pyspark.sql import SparkSession
from praw import Reddit
from azure.storage.blob import BlobServiceClient, BlobClient

# COMMAND ----------

client_id = '5XvyHnaYR2Kf-vi-8mp56w'
client_secret = 'SJ1viaX_5cHNQw3FrKUvMVa9lFII6g'
user_agent = 'my_reddit_scraper'
connection_string = 'DefaultEndpointsProtocol=https;AccountName=redditsaarul123;AccountKey=x3qJT/ZPm9AXBQeaEglfDONsAibxQ4VuWgbWtllBZShzCBO8+dTk7MWxEZBzu0qG938drUYri56V+AStNsj24w==;EndpointSuffix=core.windows.net'
container_name = 'reddit-new-data'

# Authenticating with Reddit API
reddit = Reddit(client_id=client_id,
                 client_secret=client_secret,
                 user_agent=user_agent)

# Specifying the subreddit
subreddit_name = 'dataengineering'
subreddit = reddit.subreddit(subreddit_name)

# COMMAND ----------

cluster_url = "https://adb-4621274347458844.4.azuredatabricks.net/?o=4621274347458844#setting/clusters/0219-135119-euom5qch/sparkClusterUi"

# Create SparkSession

spark = SparkSession.builder.appName("RedditDataPipeline") \
    .config("spark.master", cluster_url) \
    .getOrCreate()


# COMMAND ----------

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


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType


# COMMAND ----------

from pyspark.sql import SparkSession
from praw import Reddit
from azure.storage.blob import BlobServiceClient, BlobClient

client_id = '5XvyHnaYR2Kf-vi-8mp56w'
client_secret = 'SJ1viaX_5cHNQw3FrKUvMVa9lFII6g'
user_agent = 'my_reddit_scraper'
connection_string = 'DefaultEndpointsProtocol=https;AccountName=redditsaarul123;AccountKey=x3qJT/ZPm9AXBQeaEglfDONsAibxQ4VuWgbWtllBZShzCBO8+dTk7MWxEZBzu0qG938drUYri56V+AStNsj24w==;EndpointSuffix=core.windows.net'
container_name = 'dataextract'

# Authenticate with Reddit API
reddit = Reddit(client_id=client_id,
                 client_secret=client_secret,
                 user_agent=user_agent)

# Specify the subreddit to scrape
subreddit_name = 'dataengineering'
subreddit = reddit.subreddit(subreddit_name)

# Create SparkSession
cluster_url = "https://adb-4621274347458844.4.azuredatabricks.net/?o=4621274347458844#setting/clusters/0219-135119-euom5qch/sparkClusterUi>"

spark = SparkSession.builder.appName("RedditDataPipeline") \
    .config("spark.master", cluster_url) \
    .getOrCreate()

# Function to retrieve and process post data
def process_post_data(post):
    data = {
        "id": post.id,
        "title": post.title,
        "author": post.author.name,
        "selftext": post.selftext,
        "created_utc": post.created_utc,
        # Add more fields and processing logic as needed
        "word_count": len(post.selftext.split())  # Example processing
    }
    return data


# COMMAND ----------

# Create an empty DataFrame with defined schema
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

# Define schema for the DataFrame
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

# COMMAND ----------

data_df.show()


# COMMAND ----------

from pyspark.sql import functions as F

data_df = data_df.withColumn("created_date", F.to_date(data_df["created_utc"], "yyyy-MM-dd"))

# COMMAND ----------

data_df.count()

# COMMAND ----------

dbfs_path = f"dbfs:/data/reddit"


# COMMAND ----------

data_df.write \
    .format("csv") \
    .option("header", True) \
    .save(dbfs_path)

# COMMAND ----------


