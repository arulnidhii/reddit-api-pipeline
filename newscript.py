# Import json separately from Python's standard library
import json

# Import other libraries
from praw import Reddit
from azure.storage.blob import BlobServiceClient, BlobClient


# Replace with your credentials and connection string
client_id = 'True'
client_secret = 'True'
user_agent = 'my_reddit_scraper'
connection_string = 'True'
container_name = 'redditextracted'

# Authenticate with Reddit API
reddit = Reddit(client_id=client_id,
                client_secret=client_secret,
                user_agent=user_agent)

# Specify the subreddit to scrape
subreddit_name = 'dataengineering'
subreddit = reddit.subreddit(subreddit_name)

# Create Blob service client
blob_service_client = BlobServiceClient.from_connection_string(connection_string)


# Retrieve posts and upload data as JSON
for post in subreddit.hot(limit=100):
    data = {
        "id": post.id,
        "title": post.title,
        "author": post.author.name,
        "selftext": post.selftext,
        "created_utc": post.created_utc,
        # Add more as needed
    }

    blob_client = blob_service_client.get_blob_client(container_name, f"{subreddit_name}/{post.id}.json")
    blob_client.upload_blob(json.dumps(data))
