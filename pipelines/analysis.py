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

#function to unpickle the model
def unpickle():
        return pickle.load(open("downloaded_tokenizer.pkl", 'rb'))

#translate the sentiment to a label
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


# analysis function
def sentimentAnalysis (project_id, bucket_name, name, tweets):
    project_id = project_id
    bucket_name = bucket_name
    name = name

    logging.info("MyPredictDoFn initialisation. Load Model")
    client = storage.Client(project=project_id) #setup the storage 
    bucket = client.get_bucket(bucket_name)

    blob_model = bucket.blob('models/model.h5') # get the models from the bucket
    blob_tokenizer = bucket.blob('models/tokenizer.pkl')
       
    blob_model.download_to_filename('downloaded_model.h5') # download 
    blob_tokenizer.download_to_filename('downloaded_tokenizer.pkl')
       
    model = load_model('downloaded_model.h5') #load
    tokenizer = unpickle()

    score = 0 # init score 
    for tweet in tweets:  # tweets {useris : job, tweet: text}
        # Tokenize text
        x_test = pad_sequences(tokenizer.texts_to_sequences([tweet]), maxlen=300) # pre analyse tweet
        # Predict
        score += model.predict([x_test])[0] # predict

    avg_score = score / len(tweets)
    label = decode_sentiment(avg_score, include_neutral=True) #decode sentiment
    return [{"Name": name, "sentiment": label, "Score": float(avg_score)}]

# outputs the result
def survey(bucket_name, pre_results):
    
    results = {'Sentiment': [np.round(pre_results[0]['Score'], 2), np.round(pre_results[1]['Score'], 2)]}
    words = {'Word': [pre_results[0]['sentiment'], pre_results[1]['sentiment']]}
    category_names = [pre_results[0]['Name'], pre_results[1]['Name']]
    labels = list(results.keys())
    data = np.array(list(results.values()))
    data2 = np.array(list(words.values()))
    data_cum = data.cumsum(axis=1)
    category_colors = plt.get_cmap('bwr_r')(
        np.linspace(0.15, 0.85, data.shape[1]))

    fig, ax = plt.subplots(figsize=(9.2, 5))
    plt.margins(0,0)
    ax.invert_yaxis()
    ax.xaxis.set_visible(False)
    ax.set_xlim(0, np.sum(data, axis=1).max())

    for i, (colname, color) in enumerate(zip(category_names, category_colors)):
        widths = data[:, i]
        widths2 = data2[:, i]
        starts = data_cum[:, i] - widths
        ax.barh(labels, widths, left=starts, height=0.5,
                label=colname, color=color)
        xcenters = starts + widths / 2

        text_color = 'white'
        for y, (x, c) in enumerate(zip(xcenters, widths)):
            ax.text(x, y, str(c), ha='center', va='center',
                    color=text_color)
        for y, (x, c) in enumerate(zip(xcenters, widths2)):    
            ax.text(x, y-0.03, str(c), ha='center', va='center',
                    color=text_color, fontweight='bold')
    ax.legend(ncol=len(category_names), bbox_to_anchor=(0., 1.02, 1., .102), loc='lower left', mode="expand", borderaxespad=0., frameon=False, fontsize=15)
    
    plt.savefig('Sentiment_analysis_of_US_elections.png') # create figure
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob('results/Sentiment_analysis_of_US_elections.png')
    contents = blob.upload_from_filename('Sentiment_analysis_of_US_elections.png') # store the figure on GCP storage

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

    known_args, pipeline_args = parser.parse_known_args(argv) # set the arguments

    pipeline_options = PipelineOptions( # set the pipeline options
        flags=pipeline_args,
        project='data-engeneering-289509',
        temp_location='gs://data_engineering2020/tmp/',
        region='europe-west4')

    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
	# get all the tweets for trump
        trump_tweets = (
            p
            | 'Query Trump tweets' >> beam.io.Read(beam.io.BigQuerySource(
                query='SELECT `tweet` FROM `data-engeneering-289509.tweetdata.tweets_trump`',
                use_standard_sql=True))
	    | 'ExtractTweetsTrump' >> beam.Map(lambda elem: elem['tweet'])
        )
	
	#determine the sentiment
        trump_sentiment = (
            p
            | 'GetPID Trump' >> beam.Create([known_args.pid])
            | 'Analyse sentiment Trump' >> beam.FlatMap(
                sentimentAnalysis,
                bucket_name=known_args.mbucket,
                name="Trump",
                tweets=beam.pvalue.AsList(trump_tweets))
        )
        
	# biden tweets
        biden_tweets = (
            p
            | 'Query Biden tweets' >> beam.io.Read(beam.io.BigQuerySource(
                query='SELECT `tweet`  FROM `data-engeneering-289509.tweetdata.tweets_biden`',
                use_standard_sql=True))
            | 'ExtractTweetsBiden' >> beam.Map(lambda elem: elem['tweet'])
        )
        # biden sentiment
        biden_sentiment = (
            p
            | 'GetProjectID Biden' >> beam.Create([known_args.pid])
            | 'Analyse sentiment Biden' >> beam.FlatMap(
                sentimentAnalysis,
                bucket_name=known_args.mbucket,
                name="Biden",
                tweets=beam.pvalue.AsList(biden_tweets))
        )
        
	# combine the two results
        composed_result = (
            (trump_sentiment, biden_sentiment)
            | 'Merge sentiments' >> beam.Flatten()
        )

        (
            p
            | 'GetBucketName' >> beam.Create([known_args.mbucket])
            | 'Create Visualization' >> beam.FlatMap(survey, pre_results=beam.pvalue.AsList(composed_result)) # create the visualisation
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
