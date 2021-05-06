import pyspark
import sys
import re
import time
import json
import math as ma
import binascii as bina
import csv 
import tweepy as twp
import datetime as dt
from pyspark.streaming import StreamingContext
from itertools import combinations as comb
from collections import defaultdict,Counter



class MyStreamListener(twp.StreamListener):

    def on_status(self, status):
        
         print(status.text)

            
      
            
API_KEY = "neCN4Z66EyRWgaGeBf2Iv6Rvy"

API_key_secret= "kCgho27KXqJ9bF0M4CupwgnRHyNWnX8eVPvn1pI8p5xCE9J30D"

Access_Token = "1321971318154092544-NaGdG7mVuFjhoEOwKMoOmDvYnMaAG6"

Access_Token_Secret = "nolGzbqwqUTG82lQvRCZJySxyGh3VeI4Ac02rCSgEoPKC"

#######################################################################
"""
port_num=int(sys.argv[1]);

Authentication=twp.OAuthHandler(API_KEY,API_key_secret)

Authentication.set_access_token(Access_Token,Access_Token_Secret)


api_twp=twp.API(Authentication)


listen=MyStreamListener()

strm = twp.Stream(auth = api_twp.auth, listener=listen)

strm.filter(track=["#"])

"""














