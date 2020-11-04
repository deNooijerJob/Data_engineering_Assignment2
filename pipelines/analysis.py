
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import time
import sys
import json
import io
import urllib, base64
import numpy as np
from matplotlib import pyplot as plt

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import WriteToText as wtt
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
        # Tokenize text
        x_test = pad_sequences(tokenizer.texts_to_sequences([tweet]), maxlen=300)
        # Predict
        score += model.predict([x_test])[0]

    avg_score = score / len(tweets)
    label = decode_sentiment(avg_score, include_neutral=True) #decode sentiment
    return [{"Name": name, "sentiment": label, "Score": float(avg_score)}]

def survey(bucket_name, pre_results):
    logging.info(pre_results)
    results = {'Sentiment': [pre_results[0]['Score'], pre_results[1]['Score']]}
    category_names = [pre_results[0]['Name'], pre_results[1]['Name']]
    labels = list(results.keys())
    data = np.array(list(results.values()))
    data_cum = data.cumsum(axis=1)
    category_colors = plt.get_cmap('RdBu_r')(
        np.linspace(0.15, 0.85, data.shape[1]))

    fig, ax = plt.subplots(figsize=(9.2, 5))
    ax.invert_yaxis()
    ax.xaxis.set_visible(False)
    ax.set_xlim(0, np.sum(data, axis=1).max())

    for i, (colname, color) in enumerate(zip(category_names, category_colors)):
        widths = data[:, i]
        starts = data_cum[:, i] - widths
        ax.barh(labels, widths, left=starts, height=0.5,
                label=colname, color=color)
        xcenters = starts + widths / 2

        text_color = 'black'
        for y, (x, c) in enumerate(zip(xcenters, widths)):
            ax.text(x, y, str(c), ha='center', va='center',
                    color=text_color)
    ax.legend(ncol=len(category_names), bbox_to_anchor=(0, 1),
              loc='lower left', fontsize='small')
    
    #fig = plt.figure()
    #fig_to_upload = plt.gcf()
    
    #buf = io.BytesIO()
    plt.savefig('result.png')
    #buf.seek(0)
    #image_as_a_string = base64.b64encode(buf.read())
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob('results/result.png')
    contents = blob.upload_from_filename('result.png')

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
        project='data-engineering2020',
        temp_location='gs://data_engineering_2020/tmp/',
        region='europe-west4')

    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        trump_tweets = (
            p
            | 'Query Trump tweets' >> beam.io.Read(beam.io.BigQuerySource(
                query='SELECT `tweet` FROM `data-engineering2020.tweet_data.trump`',
                use_standard_sql=True))
	    | 'ExtractTweetsTrump' >> beam.Map(lambda elem: elem['tweet'])
        )

        trump_sentiment = (
            p
            | 'GetPID Trump' >> beam.Create([known_args.pid])
            | 'Analyse sentiment Trump' >> beam.FlatMap(
                sentimentAnalysis,
                bucket_name=known_args.mbucket,
                name="Trump",
                tweets=beam.pvalue.AsList(trump_tweets))
        )
        
        biden_tweets = (
            p
            | 'Query Biden tweets' >> beam.io.Read(beam.io.BigQuerySource(
                query='SELECT `tweet`  FROM `data-engineering2020.tweet_data.biden`',
                use_standard_sql=True))
            | 'ExtractTweetsBiden' >> beam.Map(lambda elem: elem['tweet'])
        )

        biden_sentiment = (
            p
            | 'GetProjectID Biden' >> beam.Create([known_args.pid])
            | 'Analyse sentiment Biden' >> beam.FlatMap(
                sentimentAnalysis,
                bucket_name=known_args.mbucket,
                name="Biden",
                tweets=beam.pvalue.AsList(biden_tweets))
        )
        
        composed_result = (
            (trump_sentiment, biden_sentiment)
            | 'Merge sentiments' >> beam.Flatten()
        )

        (
            p
            | 'GetBucketName' >> beam.Create([known_args.mbucket])
            | 'Create Visualization' >> beam.FlatMap(survey, pre_results=beam.pvalue.AsList(composed_result))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
