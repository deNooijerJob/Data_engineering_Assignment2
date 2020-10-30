from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import csv
import logging
import sys
import json
import time
from datetime import datetime

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import trigger

class WriteToBigQuery(beam.PTransform):
    """Generate, format, and write BigQuery table row information."""

    def __init__(self, table_name, dataset, schema, project):
        """Initializes the transform.
        Args:
          table_name: Name of the BigQuery table to use.
          dataset: Name of the dataset to use.
          schema: Dictionary in the format {'column_name': 'bigquery_type'}
          project: Name of the Cloud project containing BigQuery table.
        """
        # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
        # super(WriteToBigQuery, self).__init__()
        beam.PTransform.__init__(self)
        self.table_name = table_name
        self.dataset = dataset
        self.schema = schema
        self.project = project

    def get_schema(self):
        """Build the output table schema."""
        return ', '.join('%s:%s' % (col, self.schema[col]) for col in self.schema)

    def expand(self, pcoll):
        return (
                pcoll
                | 'ConvertToRow' >>
                beam.Map(lambda elem: {col: elem[col]
                                       for col in self.schema})
                | beam.io.WriteToBigQuery(
            self.table_name, self.dataset, self.project, self.get_schema()))

class ParseTweet(beam.DoFn):

    def __init__(self):
        # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
        # super(ParseGameEventFn, self).__init__()
        beam.DoFn.__init__(self)
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        try:
           # row = list(csv.reader([elem]))[0]
            item = json.loads(elem)
            yield {
                'user_id': item['user_id'],
                'tweet': item['text'],
                'timestamp': 12345
            }
        except:  # pylint: disable=bare-except
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem)

class ExtractTweets(beam.PTransform):

    def __init__(self, field):
        # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
        # super(ExtractAndSumScore, self).__init__()
        beam.PTransform.__init__(self)
        self.field = field

    def expand(self, pcoll):
        return (
                pcoll
                | beam.Map(lambda elem: (elem[self.field], elem['tweet']))
        )

class GetTweets(beam.PTransform):
    def __init__(self, allowed_lateness):
        beam.PTransform.__init__(self)
        self.allowed_lateness_seconds = allowed_lateness * 60

    def expand(self, pcoll):
        return (
            pcoll
            | 'TweetGlobalWindows' >> beam.WindowInto(
            beam.window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterCount(10)),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=self.allowed_lateness_seconds)
            # Extract and sum username/score pairs from the event data.
            | 'ExtractTweets' >> ExtractTweets('user_id')
        )


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    parser.add_argument('--topic', type=str, help='Pub/Sub topic to read from')
   
    parser.add_argument(
        '--subscription', type=str, help='Pub/Sub subscription to read from')
    parser.add_argument(
        '--tweet_window_duration',
        type=int,
        default=3,
        help='Numeric value of fixed window duration for tweet '
             'analysis, in minutes')
    parser.add_argument(
        '--allowed_lateness',
        type=int,
        default=6,
        help='Numeric value of allowed data lateness, in minutes')
    parser.add_argument(
        '--dataset',
        type=str,
        required=True)
    parser.add_argument(
        '--table_name')
    args, pipeline_args = parser.parse_known_args(argv)

    if args.topic is None and args.subscription is None:
        parser.print_usage()
        print(sys.argv[0] + ': error: one of --topic or --subscription is required')
        sys.exit(1)

    options = PipelineOptions(pipeline_args)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    options.view_as(SetupOptions).save_main_session = save_main_session

    # Enforce that this pipeline is always run in streaming mode
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:

        # Read from PubSub into a PCollection.
        if args.subscription:
            tweets = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(
                subscription=args.subscription)
        else:
            tweets = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(topic=args.topic)

        out_tweets = (
                tweets
                | 'DecodeString' >> beam.Map(lambda b: b.decode('utf-8'))
                | 'ParseTweets' >> beam.ParDo(ParseTweet())
                | 'AddTimestamps' >> beam.Map(
            lambda elem: beam.window.TimestampedValue(elem, elem['timestamp']))
        )

        def format_tweets(tw):
            (tweet, user_id) = tw
            return {'user_id': user_id, 'tweet': tweet}

        # Write to Bigquery
        (
            out_tweets
            | 'getTweets' >> GetTweets(args.allowed_lateness)
            | 'format' >> beam.Map(format_tweets)
            | 'store twitter posts' >> WriteToBigQuery(
                args.table_name + '_tweets',
                args.dataset, {
                    'user_id': 'STRING',
                    'tweet': 'STRING',
                },
                options.view_as(GoogleCloudOptions).project)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
