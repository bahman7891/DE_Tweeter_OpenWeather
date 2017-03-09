#!/usr/bin/env python

import boto3
import yaml
import os
from twitter import TwitterStream, OAuth
import json

def auth_func():
    # Retrieve cred for twitter api
    credentials = yaml.load(open(os.path.expanduser('./api_cred.yml')))
    twitter_stream = TwitterStream(auth = OAuth(**credentials['twitter']))

#connecting to firehose
    c = boto3.client('firehose', region_name='us-east-1')

    return c,twitter_stream

def twitter_firehose(c,twitter_stream):
    try:
        iterator = twitter_stream.statuses.sample()
        for tweet in iterator:
	    if "delete" not in tweet:
	        c.put_record(DeliveryStreamName='twitter_delivery_2',Record={'Data': json.dumps(tweet)+'\n'})
    except:
        return

if __name__ == '__main__':
    c,twitter_stream = auth_func()
    twitter_firehose(c,twitter_stream)
