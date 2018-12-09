
import os
import sys
import time
import json
import string

import re

import pyspark
import pickle

from nltk import word_tokenize

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

from Sastrawi.Stemmer.StemmerFactory import StemmerFactory

from settings import SOCKET_HOST, SOCKET_PORT
from stopwords import stopwords
from candidate import jokowi_substring, prabowo_substring

# create stemmer
factory = StemmerFactory()
stemmer = factory.create_stemmer()


def sampleWord(rdd):
    return rdd.sample(False, 0.5, 10)


def updateTotalCount(currentCount, countState):
    if countState is None:
        countState = 0
    return sum(currentCount) + countState


# def filter_tweets(tweet):
#     json_tweet = json.loads(tweet)
#     if ("extended_tweet" in json_tweet):
#         print(json_tweet["extended_tweet"]["full_text"])
#     return True

def stem_tweets(tweet):
    json_tweet = json.loads(tweet)
    tweet_data = ''

    if ("extended_tweet" in json_tweet):
        tweet_data = json_tweet["extended_tweet"]["full_text"]
    else:
        tweet_data = json_tweet["text"]

    tweet_data = re.sub(r"#(\w+)", "", tweet_data)
    tweet_data = re.sub(r"@(\w+)", "", tweet_data)
    tweet_data = re.sub(" +", " ", tweet_data)

    return stemmer.stem(tweet_data).lower()


def map_tweet_to_keyword(_tweet):
    dict_punctuation = _tweet.maketrans('', '', string.punctuation)
    tweet = _tweet.translate(dict_punctuation)
    temp_list = []
    if any(substring in tweet for substring in jokowi_substring):
        for x in word_tokenize(tweet):
            if x not in stopwords:
                temp_list.append(x)
    return temp_list


if __name__ == "__main__":
    try:
        conf = SparkConf().setAppName(
            "Twitter Presidential Candidate Tweets Listener").setMaster('local[*]')
        sparkContext = SparkContext(conf=conf)
        sparkContext.setLogLevel("ERROR")
        sparkContext.setCheckpointDir("./checkpoint")

        streamingContext = StreamingContext(sparkContext, 1)

        dstream = streamingContext.socketTextStream(SOCKET_HOST, SOCKET_PORT)

        # Micro Batches
        reduced_tweets = dstream.map(lambda tweet: stem_tweets(tweet)) \
            .flatMap(lambda tweet: map_tweet_to_keyword(tweet)) \
            .map(lambda val: (val, 1)) \
            .reduceByKey(lambda x, y: x+y)

        totalCounts = reduced_tweets.updateStateByKey(updateTotalCount)

        totalCounts.repartition(1).saveAsTextFiles("./result/result")

        totalCounts.pprint()

        streamingContext.start()
        streamingContext.awaitTermination()
    except Exception:
        streamingContext.stop()
        sys.exit()
