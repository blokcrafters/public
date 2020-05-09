#
# blokcrafters getting account status
# bid amount over 1000 > with twitter notification
#
import json
import urllib2
import time
import datetime
import dateutil.parser

# importing the module 
import tweepy

# tweet personal details 
consumer_key ="iWZ7GKeWIwGEyG2s00VfzZ2QR"
consumer_secret ="mOjCEKdBj7aTYZo40WQMYQRBOdkKgxaiTJVCPTAlhtFW5yBkf8"
access_token ="1234276592303644675-Wo9kKoOb9IpQYpBKeXEUdAo6vpxOpc"
access_token_secret ="1sBdgUCZygyNTbCZG1UV9aj7VnS91aXMsURgYJhSZktJr"
# tweet authentication of consumer key and secret 
auth = tweepy.OAuthHandler(consumer_key, consumer_secret) 

# tweet authentication of access token and secret 
auth.set_access_token(access_token, access_token_secret) 
api = tweepy.API(auth) 

url = "https://api.blokcrafters.io/v2/history/get_actions?account=eosio.token&sort=desc&simple=true"
response = urllib2.urlopen(url)
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
    #print("diff_in_min==",diff_in_min)

    action = (item['action'])
    display_symb = (item['data']['symbol'])
    bid_from = (item['data']['from'])
    bid_to = (item['data']['to'])
    memo = (item['data']['memo'])
    amt = (item['data']['amount'])
    #print("amt=",amt)

    if ( (diff_in_min < 3) and (amt > 399999) and (action == "transfer") ):
      t_msg = ("WHALE TRANSFER\n (" + str(nbr_of_tweets) + ")" + str(bid_from) + " Transfered " + str(amt) + " #WAX " +  "to " + str(bid_to) )
      #print(t_msg)
      api.update_status(status = t_msg) 
      nbr_of_tweets = nbr_of_tweets + 1
    else:
      nbr_of_tweets = nbr_of_tweets + 0

print("Total number of tweets", nbr_of_tweets)
      

