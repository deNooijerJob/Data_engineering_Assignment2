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
import nltk
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
import re

'''
    writes the tweets to a bigquery database
'''


class WriteToBigQuery(beam.PTransform):
    def __init__(self, table_name, dataset, schema, project):
        beam.PTransform.__init__(self)
        self.table_name = table_name
        self.dataset = dataset
        self.schema = schema
        self.project = project

    def get_schema(self):
        return ', '.join('%s:%s' % (col, self.schema[col]) for col in self.schema)

    def expand(self, pcoll):
        return (
                pcoll
                | 'ConvertToRow' >>
                beam.Map(lambda elem: {col: elem[col]
                                       for col in self.schema})
                | beam.io.WriteToBigQuery(
            self.table_name, self.dataset, self.project, self.get_schema()))


'''
    Initial preprocessing

    takes the input from pubsub as json and only keeps the usefull fields
'''


class ParseTweet(beam.DoFn):
    nltk.download('stopwords')
    stop_words = stopwords.words("english")
    stemmer = SnowballStemmer("english")
    invalid_chars = "@\S+|https?:\S+|http?:\S|[^A-Za-z0-9]+"

    def __init__(self):
        beam.DoFn.__init__(self)
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        try:
            item = json.loads(elem)  # reads the element

            stem = False
            text = re.sub(self.invalid_chars, ' ',
                          str(item['text']).lower()).strip()  # Remove link,user and special characters
            tokens = []
            for token in text.split():
                if token not in self.stop_words:
                    if stem:
                        tokens.append(self.stemmer.stem(token))
                    else:
                        tokens.append(token)
            pp_tweet = " ".join(tokens)

            yield {
                'tweet': pp_tweet,
                'user_id': item['user_id'],
                'time_stamp': 1234567  # time_stamp not implemented
            }

        except:  # pylint: disable=bare-except
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem)


'''
    Extract the tweets from input stream
'''


class ExtractTweets(beam.PTransform):

    def __init__(self, field):
        beam.PTransform.__init__(self)
        self.field = field

    def expand(self, pcoll):
        return (
                pcoll
                | beam.Map(lambda elem: (elem[self.field], elem['tweet']))
        )


'''
    get the initial tweets from the tweetstsream
'''


class GetTweets(beam.PTransform):
    def __init__(self, allowed_lateness):
        beam.PTransform.__init__(self)
        self.allowed_lateness_seconds = allowed_lateness * 60

    def expand(self, pcoll):
        return (
                pcoll
                | 'TweetGlobalWindows' >> beam.WindowInto(
            beam.window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterCount(50)),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=self.allowed_lateness_seconds)
                # Extract and sum username/score pairs from the event data.
                | 'ExtractTweets' >> ExtractTweets('user_id')
        )


'''
    main run loop
'''


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic_trump', type=str, help='Pub/Sub topic to read from')
    parser.add_argument('--topic_biden', type=str, help='Pub/Sub topic to read from')

    parser.add_argument(
        '--subscription_trump', type=str, help='Pub/Sub subscription to read from')
    parser.add_argument(
        '--subscription_biden', type=str, help='Pub/Sub subscription to read from')
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
    options = PipelineOptions(pipeline_args)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    options.view_as(SetupOptions).save_main_session = save_main_session
    # Enforce that this pipeline is always run in streaming mode
    options.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=options) as p:
        # Read from PubSub into a PCollection.
        if args.subscription_trump or args.subscription_biden:
            tweets_trump = p | 'ReadPubSub_trump' >> beam.io.ReadFromPubSub(
                subscription_trump=args.subscription_trump)
            tweets_biden = p | 'ReadPubSub_biden' >> beam.io.ReadFromPubSub(
                subscription_biden=args.subscription_biden)
        else:
            tweets_trump = p | 'ReadPubSub_trump' >> beam.io.ReadFromPubSub(topic=args.topic_trump)
            tweets_biden = p | 'ReadPubSub_biden' >> beam.io.ReadFromPubSub(topic=args.topic_biden)
        '''
            first steps in the pipline
        '''       
        out_tweets_Trump = (
                tweets_trump
                | 'DecodeString Trump' >> beam.Map(lambda b: b.decode('utf-8'))  # make sure that the tweets are in utf-8 base
                | 'ParseTweets Trump' >> beam.ParDo(ParseTweet())  # parse all the tweets

        )
        out_tweets_Biden = (
                tweets_biden
                | 'DecodeString Biden' >> beam.Map(lambda b: b.decode('utf-8'))  # make sure that the tweets are in utf-8 base
                | 'ParseTweets Biden' >> beam.ParDo(ParseTweet())  # parse all the tweets

        )

        def format_tweets(tw):
            (user_id, tweet) = tw
            return {'user_id': user_id, 'tweet': tweet}  # timestamp not implemented

        # Write to Bigquery
        (
                out_tweets_Trump
                | 'getTweets Trump' >> GetTweets(args.allowed_lateness)  # get the tweets
                | 'format output Trump' >> beam.Map(format_tweets)  # format the tweets
                | 'store twitter posts Trump' >> WriteToBigQuery(  # write them to the db
            args.table_name + '_trump',
            args.dataset, {
                'tweet': 'STRING',
                'user_id': 'STRING'  # timestamp not implemented
            },
            options.view_as(GoogleCloudOptions).project)
        )
        (
                out_tweets_Biden
                | 'getTweets Biden' >> GetTweets(args.allowed_lateness)  # get the tweets
                | 'format output Biden' >> beam.Map(format_tweets)  # format the tweets
                | 'store twitter posts Biden' >> WriteToBigQuery(  # write them to the db
            args.table_name + '_biden',
            args.dataset, {
                'tweet': 'STRING',
                'user_id': 'STRING'  # timestamp not implemented
            },
            options.view_as(GoogleCloudOptions).project)
        )


'''
    run the pipeline
'''
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()




