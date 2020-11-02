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


class Sentiment_Analysis(beam.DoFn):
    def __init__(self):
        beam.DoFn.__init__(self)
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, tweets):
        result = None
        try:
            result = ['{ "accuracy": %0.3f,  "loss": %0.3f }' % (1, 1)]
        except:
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', tweets)
        return result



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
        trump_sentiment = (
            p
            | 'Query trump tweets' >> beam.io.Read(beam.io.BigQuerySource(
                query='SELECT * FROM `data-engeneering-289509.tweetdata.trump`',
                use_standard_sql=True))
            | 'Analyse sentiment' >> beam.parDo(Sentiment_Analysis())
        )

        trump_sentiment = (
                p
                | 'Query trump tweets' >> beam.io.Read(beam.io.BigQuerySource(
            query='SELECT * FROM `data-engeneering-289509.tweetdata.biden`',
            use_standard_sql=True))
                | 'Analyse sentiment' >> beam.parDo(sentiment_Analysis())
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()