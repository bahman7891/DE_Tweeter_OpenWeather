#!/usr/bin/env python

import sys
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession , Row
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
import json
from textblob import TextBlob

APP_NAME = 'Climate Sentiment Stream'

def cloudiness_pair(rec_dict):
'''
input: an RDD of dictionaries.
returns: (key,value) pair: key = cloudiness level, value = tweet text

'''

    cldness = rec_dict.get('cloudiness')
    text = rec_dict.get('text')
    if cldness < 25:
        return ('L',text)
    elif 25 <= cldness < 70:
        return ('M',text)
    else:
        return ('H',text)


# def cld_sent(paired_data):
#     cld_list = []
#     pol_list = []
#     Record = Row("cloudiness", "polarity")
#     for cld,text in paired_data:
#         blob = TextBlob(text)
#         pol_list.append(blob.polarity)
#         cld_list.append(cld)
# #    cld_df = pd.DataFrame({'cloud_level':cld_list,'polarity':pol_list})
#     cld_df = spark.createDataFrame([Record(cld_list[i],pol_list[i]) for i in range(len(cond_list))])
#     return cld_df




def condition_pair(rec_dict):
'''
input: an RDD of dictionaries.
returns: (key,value) pair: key = weather condition, value = tweet text

'''

    cond = rec_dict.get('condition')
    text = rec_dict.get('text')
    return (cond,text)

# def cond_sent_df(paired_data,sqlContext):
#     spark = sqlContext
#     cond_list = []
#     pol_list = []
#     Record = Row("condition", "polarity")
#     for cond,text in paired_data:
#         blob = TextBlob(text)
#         pol_list.append(blob.polarity)
#         cond_list.append(cond)
#     #cond_df = pd.DataFrame({'condition':cond_list,'polarity':pol_list})
#     cond_df = spark.createDataFrame([Record(cond_list[i],pol_list[i]) for i in range(len(cond_list))])
#     return cond_df

def text_reduce(text_1,text_2):
'''
input: two strings
returns: unique set of the elements of the union of the two strings.

'''


    text_1 = text_1.strip()
    text_1 = text_1.split()
    text_2 = text_2.strip()
    text_2 = text_2.split()
    text = text_1 + text_2
    text_set = set(text)
    return ' '.join(text_set)


def to_df(spark,col,paired_data):
'''
input:
    spark = sparksession instant
    col = the column name (weather or any other feature) : string.
    paired_data: pair of (key,value) in which key and value are both string.

returns: spark sql dataframe with columns: col, and polarity of the value (text).

'''

    w_list = []
    pol_list = []
    Record = Row(col, 'polarity')
    for w,text in paired_data:
        blob = TextBlob(text)
        pol_list.append(blob.polarity)
        w_list.append(w)
    #cond_df = pd.DataFrame({'condition':cond_list,'polarity':pol_list})
    w_df = spark.createDataFrame([Record(w_list[i],pol_list[i]) for i in range(len(w_list))])
    return w_df





def read_data(spark):
'''
Reads data from AWS S3 storage.
Returns: raw_data in json.
'''

#    raw_tweets = spark.read.json('s3a://climatesentimenttweet/2017/03/05/*/*')
    raw_weather = spark.read.json('s3a://climatesentimentdata/2017/*/*/*/*')
    raw_tweets = spark.read.json('s3a://climatesentimenttweet/2017/03/05/*/*')

    return raw_tweets,raw_weather


if __name__ == "__main__":
        # Configure OPTIONS
    #conf = SparkConf().setAppName(APP_NAME)
    #conf = conf.setMaster("local[*]")
    #in cluster this will be like
    #"spark://ec2-0-17-03-078.compute-#1.amazonaws.com:7077"
    #sc = SparkContext(conf=conf)
    #sqlContext = SQLContext(sc)

    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    spark = SparkSession.builder \
        .master("local") \
        .appName(APP_NAME) \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()



    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)
    log.warn('Custom Logging message that you can see on the console')

    #Enter the name for a new directory (in quotes):
    #With the following order:
        # cloudiness,condition
    cld_fname = sys.argv[1]
    cond_fname = sys.argv[2]




    # Execute Main functionality



    # read raw data from AWS S3 buckets
    raw_tweets,raw_weather = read_data(spark)

    # cache for speed
    raw_tweets.cache();
    raw_weather.cache();

    # filter the empty tweets
    raw_tweets_filtered = raw_tweets.filter('id is not null')
    # create table attributes of the tweet user
    user_df = raw_tweets_filtered.selectExpr('user.id AS user_id','place.country_code AS tweet_country','place.name AS tweet_city').distinct()
    # clean the user attribute table
    user_df_clean = user_df.filter('tweet_country is not null and tweet_city is not null')
    # create table of tweet text attributes
    tweet_df = raw_tweets_filtered.selectExpr('id AS tweet_id', 'user.id AS user_id', 'text','lang AS language')



    # create table of weather attributes
    weather_data = raw_weather.select(explode(raw_weather.list)).selectExpr("col.*")
    w_df = weather_data.selectExpr('name AS user_city','sys.country AS user_country','weather.description[0] AS condition',
                              'weather.main[0] AS parameter','main.temp AS temperature','main.pressure AS pressure',
                              'main.humidity AS humidity',
                              'wind.speed AS wind_speed',
                              'clouds.all AS cloudiness')

    # register the table to make queries out of
    tweet_df.registerTempTable('tweets')
    # english language tweets
    en_tweets_df = spark.sql(sqlQuery='SELECT * FROM tweets WHERE language="en"')
    # join with the user attributes to have both tweets and their location
    en_tweet_joined = en_tweets_df.join(user_df,en_tweets_df.user_id == user_df.user_id)
    # filter agian for null location
    en_tweet_all = en_tweet_joined.filter('tweet_city is not null')
    en_tweet_all = en_tweet_all.filter('tweet_country is not null')
    # register the tweets
    en_tweet_all.registerTempTable('en_tweets')

    # JOIN WITH THE WEATHER DATA BASED ON THE LOCATION : CITY
    tweet_w_df = en_tweet_all.join(w_df,w_df.user_city == en_tweet_all.tweet_city).distinct()

    # create an RDD out of the data frame. Each RDD would be a collection of dictionaries.
    tweet_w_rdd = tweet_w_df.rdd
    # map them to python dictionaries.
    rdd_dict = tweet_w_rdd.map(lambda x:x.asDict())


    # ANALYSIS OF CLOUDINESS:

    # categorize the cloudiness into three categories.
    rdd_cld = rdd_dict.map(cloudiness_pair)

    # reduce all text in the same category.
    rdd_cld_red = rdd_cld.reduceByKey(text_reduce)

    #print rdd_cld_red.take(1)
    # action to collect all:
    cld_text = rdd_cld_red.collect()

    #print cld_text[0]
    # create spark dataframe from the results.
    cloudiness_sent_df = to_df(spark,'cloudiness',cld_text)
    # coalesce and write in AWS S3 in CSV format.
    cloudiness_sent_df.coalesce(1).write.csv('s3a://climsentdata/'+cld_fname)



    # ANALYSIS OF WHEATHER CODITION:

    # create pair
    rdd_cond = rdd_dict.map(condition_pair)
    rdd_cond_red = rdd_cond.reduceByKey(text_reduce)
    cond_collect = rdd_cond_red.collect()

    cond_df = to_df(spark,'condition',cond_collect)
    cond_df.coalesce(1).write.csv('s3a://climsentdata/'+cond_fname)
