import os
import sys
import json
import string
from time import time
import re

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext, DStream

import matplotlib

from nltk import word_tokenize
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
from stopwords import stopwords

from candidate import jokowi_substring, prabowo_substring
from settings import SOCKET_HOST, SOCKET_PORT

matplotlib.use('Agg')

# create stemmer
factory = StemmerFactory()
stemmer = factory.create_stemmer()
labels = ["Jokowi", "Prabowo"]


def sampling(rdd):
    return rdd.sample(False, 0.5, 10)


def count_tweets(rdd, accumulator):
    rdd.foreach(lambda _: accumulator.add(1))


def count_keywords(rdd, accumulator):
    rdd.foreach(lambda record: accumulator.add(record[1]))


def update_total_count(currentCount, countState):
    if countState is None:
        countState = 0
    return sum(currentCount) + countState


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


def map_and_filter_tweet_to_keyword(_tweet, substrings=[]):
    dict_punctuation = _tweet.maketrans('', '', string.punctuation)
    tweet = _tweet.translate(dict_punctuation)
    temp_list = []
    if any(substring in tweet for substring in substrings):
        for x in word_tokenize(tweet):
            if x not in stopwords:
                temp_list.append(x)
    return temp_list


conf = SparkConf().setAppName(
    "Twitter Presidential Candidate Tweets Listener").setMaster('local[*]')

sparkContext = SparkContext(conf=conf)
sparkContext.setLogLevel("ERROR")
sparkContext.setCheckpointDir("./checkpoint")

streamingContext = StreamingContext(sparkContext, 1)

# try:
dstream = streamingContext.socketTextStream(SOCKET_HOST, SOCKET_PORT)
sampled = dstream.transform(sampling)

# Micro Batches

# Tweets Stream about Jokowi
count_tweets_jokowi = sparkContext.accumulator(0)
count_keywords_jokowi = sparkContext.accumulator(0)

tweets_jokowi = sampled.map(lambda tweet: stem_tweets(tweet))

tweets_jokowi.foreachRDD(
    lambda rdd: count_tweets(rdd, count_tweets_jokowi))

keywords_jokowi = tweets_jokowi.flatMap(
    lambda tweet: map_and_filter_tweet_to_keyword(tweet, jokowi_substring)) \
    .map(lambda val: (val, 1)) \
    .reduceByKey(lambda x, y: x + y)

keywords_jokowi.foreachRDD(
    lambda rdd: count_keywords(rdd, count_keywords_jokowi))

total_counts_jokowi = keywords_jokowi.updateStateByKey(
    update_total_count)

total_counts_jokowi \
    .repartition(1) \
    .saveAsTextFiles("./result/jokowi/result")

total_counts_jokowi.pprint()
# End of tweets stream about Jokowi

# Tweets Stream about Prabowo
count_tweets_prabowo = sparkContext.accumulator(0)
count_keywords_prabowo = sparkContext.accumulator(0)

tweets_prabowo = sampled.map(lambda tweet: stem_tweets(tweet))

tweets_prabowo.foreachRDD(
    lambda rdd: count_tweets(rdd, count_tweets_prabowo))

keywords_prabowo = tweets_prabowo.flatMap(
    lambda tweet: map_and_filter_tweet_to_keyword(tweet, prabowo_substring)) \
    .map(lambda val: (val, 1)) \
    .reduceByKey(lambda x, y: x + y)

keywords_prabowo.foreachRDD(
    lambda rdd: count_keywords(rdd, count_keywords_prabowo))

total_counts_prabowo = keywords_prabowo.updateStateByKey(
    update_total_count)

total_counts_prabowo \
    .repartition(1) \
    .saveAsTextFiles("./result/prabowo/result")

total_counts_prabowo.pprint()
# End of tweets stream about Prabowo

# End of micro batches

streamingContext.start()
streamingContext.awaitTerminationOrTimeout(15)

# with open("./result/summary.txt", "w") as file:
#     file.write("JOKOWI\n")
#     file.write("TWEETS: " + str(count_tweets_jokowi.value) + "\n")
#     file.write("KEYWORDS: " + str(count_keywords_jokowi.value) + "\n")
#     file.write("PRABOWO\n")
#     file.write("TWEETS: " + str(count_tweets_prabowo.value) + "\n")
#     file.write("KEYWORDS: " + str(count_keywords_prabowo.value) + "\n")


# count_tweets_value = [count_tweets_jokowi.value,
#                       count_keywords_prabowo.value]
# count_keywords_value = [
#     count_keywords_jokowi.value, count_keywords_jokowi.value]

# fig, (ax1, ax2) = matplotlib.pyplot.subplots(1, 2)
# ax1.pie(count_tweets_value,  labels=labels,
#         shadow=True, autopct='%1.0f%%')
# ax1.set(aspect="equal", title="Tweets")

# ax2.pie(count_keywords_value, labels=labels,
#         shadow=True, autopct='%1.0f%%')
# ax2.set(aspect="equal", title="Keywords")
# matplotlib.pyplot.pause(0.0001)

# fig.savefig("./result/result-" + time() + ".png")
