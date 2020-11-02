from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import sys
import json

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import trigger


def Sentiment_Analysis(trump_tweets, biden_tweets):
    return ['{ "accuracy": %0.3f,  "loss": %0.3f }' % (1, 1)]



def run ( argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(
        flags=pipeline_args,
        project='data-engeneering-289509',
        temp_location='gs://data_engineering2020/tmp/',
        region='europe-west4')

    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        trump_data = (
            p | 'Query trump tweets' >> beam.io.Read(beam.io.BigQuerySource(
                query='SELECT * FROM `data-engeneering-289509.tweetdata.trump`',
                use_standard_sql=True))
        )

        biden_data = (
            p | 'Query biden Tweets' >> beam.io.Read(beam.io.BigQuerySource(
                query='SELECT * FROM `data-engeneering-289509.tweetdata.biden`',
                use_standard_sql=True))
        )

        sentiment = (
            p | 'Predict Sentiment' >> beam.FlatMap(
                    Sentiment_Analysis, trump_tweets=trump_data, biden_tweets=biden_data
                )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()