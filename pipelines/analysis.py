from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import time
import sys
import json

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import storage 
import pickle
from keras.preprocessing.sequence import pad_sequences
from keras.models import load_model
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import trigger

def unpickle():
        return pickle.load(open("downloaded_tokenizer.pkl", 'rb'))


def decode_sentiment(score, include_neutral=True):
    # SENTIMENT
    POSITIVE = "POSITIVE"
    NEGATIVE = "NEGATIVE"
    NEUTRAL = "NEUTRAL"
    SENTIMENT_THRESHOLDS = (0.4, 0.7)
    if include_neutral:
        label = NEUTRAL
        if score <= SENTIMENT_THRESHOLDS[0]:
            label = NEGATIVE
        elif score >= SENTIMENT_THRESHOLDS[1]:
            label = POSITIVE

        return label
    else:
        return NEGATIVE if score < 0.5 else POSITIVE



def sentimentAnalysis (project_id, bucket_name, name, tweets):
    project_id = project_id
    bucket_name = bucket_name
    name = name

    logging.info("MyPredictDoFn initialisation. Load Model")
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)

    blob_model = bucket.blob('models/model.h5')
    blob_tokenizer = bucket.blob('models/tokenizer.pkl')
       
    blob_model.download_to_filename('downloaded_model.h5')
    blob_tokenizer.download_to_filename('downloaded_tokenizer.pkl')
       
    model = load_model('downloaded_model.h5')
    tokenizer = unpickle()

    score = 0
    for tweet in tweets:  # tweets {useris : job, tweet: text}
        logging.info(tweet)
        # Tokenize text
        x_test = pad_sequences(tokenizer.texts_to_sequences([tweet['text']]), maxlen=300)
        # Predict
        score += model.predict([x_test])[0]
        # Decode sentiment

    avg_score = score / len(tweets)
    label = decode_sentiment(avg_score, include_neutral=True)

    return {"name": name, "General Sentiment": label, "Average Score": float(avg_score)}




def run ( argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--pid',
        dest='pid',
        help='project id')

    parser.add_argument(
        '--mbucket',
        dest='mbucket',
        help='model bucket name')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(
        flags=pipeline_args,
        project='data-engeneering-289509',
        temp_location='gs://data_engineering2020/tmp/',
        region='europe-west4')

    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        trump_tweets = (
            p
            | 'Query trump tweets' >> beam.io.Read(beam.io.BigQuerySource(
                query='SELECT `tweet` FROM `data-engeneering-289509.tweetdata.trump`',
                use_standard_sql=True))
        )

        trump_sentiment = (
                p
                | 'Analyse sentiment trump' >> beam.FlatMap(
                    sentimentAnalysis,
                    project_id=known_args.pid,
                    bucket_name=known_args.mbucket,
                    name="Trump",
                    tweets=beam.pvalue.AsList(trump_tweets))
        )

        biden_tweets = (
                p
                | 'Query biden tweets' >> beam.io.Read(beam.io.BigQuerySource(
            query='SELECT `tweet` FROM `data-engeneering-289509.tweetdata.biden`',
            use_standard_sql=True))
        )

        biden_sentiment = (
                p
                | 'Analyse sentiment trump' >> beam.FlatMap(
                    sentimentAnalysis,
                    project_id=known_args.pid,
                    bucket_name=known_args.mbucket,
                    name="Biden",
                    tweets=beam.pvalue.AsList(biden_tweets))
        )

        composed_result = ((trump_sentiment, biden_sentiment) | 'Merge sentiments' >> beam.Flatten())
        logging.info(composed_result)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
