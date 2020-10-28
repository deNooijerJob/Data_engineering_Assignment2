from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import csv
import logging
import sys
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

class parseTweet(beam.DoFn):
    """Parses the raw game event info into a Python dictionary.

    Each event line has the following format:
      username,teamname,score,timestamp_in_ms,readable_time

    e.g.:
      user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224

    The human-readable time string is not used here.
    """

    def __init__(self):
        # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
        # super(ParseGameEventFn, self).__init__()
        beam.DoFn.__init__(self)
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        try:
            row = list(csv.reader([elem]))[0]
            yield {
                'user_id': row[1],
                'tweet': row[0],
                'timestamp': row[3]
            }
        except:  # pylint: disable=bare-except
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem)


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
                | 'ParseGameEventFn' >> beam.ParDo(parseTweet())
                | 'AddEventTimestamps' >> beam.Map(
            lambda elem: beam.window.TimestampedValue(elem, elem['timestamp']))
        )

        def format_tweets(tw):
            (tweet, user_id) = tw
            return {'user_id': user_id, 'tweet': tweet}

        # Write to Bigquery
        (
            out_tweets
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
