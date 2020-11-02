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
        predict_data = (
                p | 'Query Table' >> beam.io.Read(beam.io.BigQuerySource(
                    query='SELECT * FROM `data-engeneering-289509.tweetdata.sentiment_tweets`',
                    use_standard_sql=True))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()