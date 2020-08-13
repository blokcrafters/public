#!/usr/bin/env python3
#
# blokcrafters getting account status
# bid amount over 1000 > with twitter notification
#
import json
import urllib.request
import time
import datetime
import dateutil.parser

# importing the module 
import tweepy

# tweet personal details 
consumer_key ="iW....QR"
consumer_secret ="mO....f8"
access_token ="12....pc"
access_token_secret ="1s....Jr"
# tweet authentication of consumer key and secret 
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

# tweet authentication of access token and secret 
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

url = "https://api.blokcrafters.io/v2/history/get_actions?account=eosio.names&sort=desc&simple=true"
response = urllib.request.urlopen(url)
data = response.read()
json_result = json.loads(data)
#now time_stamp 
now = datetime.datetime.now()
#print (now.strftime("%Y-%m-%dT%H:%M:%S.000"))
nbr_of_tweets = 0
for item in json_result['simple_actions']:
    #trade time stamp
    trade_ts = (item['timestamp'])
    #convert to date_object
    trade_ts = dateutil.parser.parse(trade_ts)
    #diff between now and trade_time Hr: Min: Sec: 
    time_diff = (now - trade_ts)
    minutes = divmod(time_diff.seconds, 60) 
    #print('Difference in minutes: ', minutes[0], 'minutes', minutes[1], 'seconds')
    diff_in_min = minutes[0]
    #print(diff_in_min)

    display_symb = (item['data']['symbol'])
    bid_from = (item['data']['from'])
    bid_to = (item['data']['to'])
    memo = (item['data']['memo'])
    amt = (item['data']['amount'])
   
    if ( (diff_in_min < 7) and (amt > 4999) and (memo.startswith('bid name ')) ):
      t_msg = ("NAME BID \n" + str(bid_from) + " Paid " + str(amt) + " #WAX " +  "to " + str(bid_to) + " for the name " + str(memo).replace('bid name ','') )
      #print(t_msg)
      api.update_status(status = t_msg) 
      nbr_of_tweets = nbr_of_tweets + 1
    else:
      nbr_of_tweets = nbr_of_tweets + 0

print("Total number of tweets", nbr_of_tweets)
      

