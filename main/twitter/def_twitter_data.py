import tweepy
import json
import re
from datetime import datetime
from aws_services.def_boto3_aws import write_log_dynamodb
import pandas as pd
import boto3


# Yaniv:
consumer_key = '******************'
consumer_secret = '******************'
access_token = '******************'
access_secret = '******************'
bearer_token = '******************'


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth)
client = tweepy.Client(bearer_token=bearer_token)

# to remove non-ascii chars like emojies
def remove_non_ascii(text):
    return re.sub(r'[^\x00-\x7F]+', '', text)


# Replace the limit=1000 with the maximum number of Tweets you want

def get_tweets_data(word, num_of_tweets=5):
    try:
        tweets = []
        likes = []
        tweet_time = []
        source = []
        tweets_id = []
        query = word + ' -is:retweet'

        for tweet in tweepy.Paginator(client.search_recent_tweets, query=query,
                                      tweet_fields=['context_annotations', 'created_at', 'text', 'id'],
                                      max_results=10).flatten(limit=num_of_tweets):
            tweets_id.append(tweet.id)
            tweet_time.append(tweet.created_at)
            likes_count = api.get_status(tweet["id"]).favorite_count
            likes.append(likes_count)
            tweet.text = remove_non_ascii(tweet.text)
            tweets.append(tweet.text.encode("ascii", "ignore").decode())
            source.append('twitter')

        df = pd.DataFrame({'id': tweets_id, 'time': tweet_time, 'likes': likes, 'text': tweets, 'source': source})
        # df['time'] = df['time'].dt.strftime('%d-%m-%y %H:%M:%S')
        data = df.to_json(orient='index', indent=4)
        return data
    except Exception as e:
        write_log_dynamodb('get_tweets_data', e)
        return(f"Get tweets data - something went wrong : {e}")
