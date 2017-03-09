#!/usr/bin/env python


import urllib2, urllib
import os
import yaml
import boto3
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import json
import datetime



def weather_data(city_ids):
    '''
    Input: At the moment you need to insert the name of the city and country in the code.
    Output: the api response.
    '''
    
    credentials = yaml.load(open(os.path.expanduser('~/api_cred.yml')))
    appid = credentials['openweathermap']

    baseurl = 'http://api.openweathermap.org/data/2.5/group?id='
    ids=''
    for cid in city_ids:
	ids = ids + ',' + cid
    yql_url = baseurl +ids+'&units=metric'+'&appid='+appid+"&format=json"
    try:
    	result = urllib2.urlopen(yql_url)
        data = json.load(result)

        c = boto3.client('firehose', region_name='us-east-1')
        c.put_record(DeliveryStreamName='climatesentiment_weather_delivery',Record={'Data': json.dumps(data)+'\n'})
	f = open('logs_new.txt','a')
	f.write('\n success\n')
    except urllib2.HTTPError:
	f = open('logs_new.txt','a')
	f.write('\n Urlib2.HTTPError\n')
        pass 
 		

if __name__ == '__main__':
    city_ids = []	
    with open('city_ids') as f:
	for line in f:
		line = line.strip()
    		city_ids.append(line)
		
    weather_data(city_ids)
