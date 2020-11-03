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


class Sentiment_Analysis(beam.DoFn):
    def __init__(self, project_id, bucket_name):
        # SENTIMENT
        self.POSITIVE = "POSITIVE"
        self.NEGATIVE = "NEGATIVE"
        self.NEUTRAL = "NEUTRAL"
        self.SENTIMENT_THRESHOLDS = (0.4, 0.7)
        self._model = None
        self._tokenizer = None
        self._project_id = project_id
        self._bucket_name = bucket_name
    
    def unpickle(self):
        return pickle.load(open("downloaded_tokenizer.pkl", 'rb'))    
   
    def setup(self):
        logging.info("MyPredictDoFn initialisation. Load Model")
        client = storage.Client(project=self._project_id)
        bucket = client.get_bucket(self._bucket_name)

        blob_model = bucket.blob('models/model.h5')
        blob_tokenizer = bucket.blob('models/tokenizer.pkl')
       
        blob_model.download_to_filename('downloaded_model.h5')
        blob_tokenizer.download_to_filename('downloaded_tokenizer.pkl')
       
        self._model = load_model('downloaded_model.h5')
        self._tokenizer = self.unpickle()
       

    def decode_sentiment(self, score, include_neutral=True):
        if include_neutral:
            label = self.NEUTRAL
            if score <= self.SENTIMENT_THRESHOLDS[0]:
                label = self.NEGATIVE
            elif score >= self.SENTIMENT_THRESHOLDS[1]:
                label = self.POSITIVE

            return label
        else:
            return self.NEGATIVE if score < 0.5 else self.POSITIVE


    def process(self, tweets):
        score = 0
        for tweet in tweets:
            # Tokenize text
            x_test = pad_sequences(self._tokenizer.texts_to_sequences([tweet]), maxlen=300)
            # Predict
            score += self._model.predict([x_test])[0]
            # Decode sentiment

        avg_score = score / len(tweets)
        label = self.decode_sentiment(avg_score, include_neutral=True)

        return {"General Sentiment": label, "Average Score": float(avg_score)}



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
        trump_sentiment = (
            p
            | 'Query trump tweets' >> beam.io.Read(beam.io.BigQuerySource(
                query='SELECT * FROM `data-engeneering-289509.tweetdata.trump`',
                use_standard_sql=True))
            | 'Analyse sentiment trump' >> beam.ParDo(Sentiment_Analysis(project_id=known_args.pid,
                                                                          bucket_name=known_args.mbucket))
        )

        biden_sentiment = (
                p
                | 'Query biden tweets' >> beam.io.Read(beam.io.BigQuerySource(
            query='SELECT * FROM `data-engeneering-289509.tweetdata.biden`',
            use_standard_sql=True))
                | 'Analyse sentiment biden' >> beam.ParDo(Sentiment_Analysis(project_id=known_args.pid,
                                                                        bucket_name=known_args.mbucket))
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
