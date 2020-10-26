
# coding: utf-8
import datetime
import json
import time

import tweepy
from google.cloud import pubsub_v1
from tweepy.streaming import StreamListener
auth = tweepy.OAuthHandler("XMeLenEr8FQeeczRi1q2ZYRZx", "naOsHhaGkNSysW3btmsog4ioKz0vS5eNdxsxOvAGGO8Kx7wwG4") # fill in the keys
auth.set_access_token("XMeLenEr8FQeeczRi1q2ZYRZx", "naOsHhaGkNSysW3btmsog4ioKz0vS5eNdxsxOvAGGO8Kx7wwG4")
# fill in the keys

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=False)
hastags = [] # add hashtags

# Method to push messages to pubsub
def write_to_pubsub(data):
    try:
        if data["lang"] == "en":
            publisher.publish(topic_path, data=json.dumps({
                "text": data["text"],
                "user_id": data["user_id"],
                "id": data["id"],
                "posted_at": datetime.datetime.fromtimestamp(data["created_at"]).strftime('%Y-%m-%d %H:%M:%S')
            }).encode("utf-8"), tweet_id=str(data["id"]).encode("utf-8"))
    except Exception as e:
        raise

# Method to format a tweet from tweepy
def reformat_tweet(tweet):
    x = tweet

    processed_doc = {
        "id": x["id"],
        "lang": x["lang"],
        "retweeted_id": x["retweeted_status"]["id"] if "retweeted_status" in x else None,
        "favorite_count": x["favorite_count"] if "favorite_count" in x else 0,
        "retweet_count": x["retweet_count"] if "retweet_count" in x else 0,
        "coordinates_latitude": x["coordinates"]["coordinates"][0] if x["coordinates"] else 0,
        "coordinates_longitude": x["coordinates"]["coordinates"][0] if x["coordinates"] else 0,
        "place": x["place"]["country_code"] if x["place"] else None,
        "user_id": x["user"]["id"],
        "created_at": time.mktime(time.strptime(x["created_at"], "%a %b %d %H:%M:%S +0000 %Y"))
    }

    if x["entities"]["hashtags"]:
        processed_doc["hashtags"] = [{"text": y["text"], "startindex": y["indices"][0]} for y in
                                     x["entities"]["hashtags"]]
    else:
        processed_doc["hashtags"] = []

    if x["entities"]["user_mentions"]:
        processed_doc["usermentions"] = [{"screen_name": y["screen_name"], "startindex": y["indices"][0]} for y in
                                         x["entities"]["user_mentions"]]
    else:
        processed_doc["usermentions"] = []

    if "extended_tweet" in x:
        processed_doc["text"] = x["extended_tweet"]["full_text"]
    elif "full_text" in x:
        processed_doc["text"] = x["full_text"]
    else:
        processed_doc["text"] = x["text"]

    return processed_doc

class TweetListener (StreamListener):
    def __init__(self):
        super(StdOutListener, self).__init__()

    def on_status(self, data):
        write_to_pubsub(reformat_tweet(data._json))

    def on_error(self, status):
        if status == 420:
            print("rate limit active")
            return False

listener = TweetListener()

stream = tweepy.Stream(auth, listener, tweet+mode='extended')
stream.filter(track=hastags)