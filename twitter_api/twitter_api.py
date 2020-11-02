# coding: utf-8
import datetime
import json
import time

import tweepy
from google.cloud import pubsub_v1
from tweepy.streaming import StreamListener
import credentials

# Config
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("data-engeneering-289509", "tweety") # TODO two topics (also in GCP)



auth = tweepy.OAuthHandler(credentials.CONSUMER_KEY, credentials.CONSUMER_SECRET)
auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_SECRET)

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=False)

# Define the list of terms to listen to
trump_hashtags = ["#covid", "#COVID", "#Covid", "#covid19"] # TODO change this
biden_hastags = ["#covid", "#COVID", "#Covid", "#covid19"]

# Method to push messages to pubsub # TODO duplicate for biden / trump
def write_to_pubsub(data):
    try:
        if data["lang"] == "en":
            publisher.publish(topic_path, data=json.dumps({ # TODO change the output fields according to model
                "text": data["text"],
                "user_id": data["user_id"],
                "id": data["id"],
                "posted_at": datetime.datetime.fromtimestamp(data["created_at"]).strftime('%Y-%m-%d %H:%M:%S')
            }).encode("utf-8"), tweet_id=str(data["id"]).encode("utf-8"))
    except Exception as e:
        raise

# Method to format a tweet from tweepy # TODO dont touch this
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

# Custom listener class
class StdOutListener(StreamListener): # Don't touch this
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just pushes tweets to pubsub
    """
    topic = None

    def setTopic(self, string):
        self.topic = string

    def __init__(self):
        super(StdOutListener, self).__init__()
        self._counter = 0

    def on_data(self,data):
        data = json.loads(data)
        if self.topic == 'trump':
            write_to_pubsub(reformat_tweet(data))
        elif self.topic == 'biden':
            write_to_pubsub(reformat_tweet(data)) # TODO maak dit een biden functie
        self._counter += 1
        return True

    def on_error(self, status):
        if status == 420:
            print("rate limit active")
            return False


# Start listening
trump_api = StdOutListener()
trump_api.setTopic('trump')
stream_trump = tweepy.Stream(auth, trump_api, tweet_mode='extended')
stream_trump.filter(track=trump_hashtags)

# Start listening
biden_api = StdOutListener()
biden_api.setTopic('biden')
stream_biden = tweepy.Stream(auth, biden_api, tweet_mode='extended')
stream_biden.filter(track=biden_hashtags)

