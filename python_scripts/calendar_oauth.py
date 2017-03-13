import requests
import json
from pprint import pprint
import unicodecsv as csv
from datetime import datetime
from time import sleep
from yelp.client import Client
from yelp.oauth1_authenticator import Oauth1Authenticator
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from front_v2_etl_campaigns import get_from_dict,flatten_data,write_to_csv


<<<<<<< HEAD
=======
with open("../calendar.dat") as file:
    google_auth=json.load(file)

client_id=google_auth['client_id']
client_secret=google_auth['client_secret']
redirect_uri=google_auth['redirect_uri']
authorization_base_url = google_auth['authorization_base_url']
token_url = "https://www.googleapis.com/oauth2/v4/token"
>>>>>>> changed auth
scope=['https://www.googleapis.com/auth/calendar.readonly']
#client=BackendApplicationClient(client_id=client_id)
#oauth=OAuth2Session(client=client)
#token=oauth.fetch_token(token_url='https://api.yelp.com/oauth2/token',client_id=client_id,client_secret=client_secret)

#reviews=oauth.get('https://api.yelp.com/v3/businesses/eatsa-san-francisco-2/reviews')
#pprint(reviews.content)

params={
"calendarId":"eatsa.com_baequihu4jrcoo2l328efku5mk@group.calendar.google.com",
"pageToken":None,
}
url='//www.googleapis.com/auth/calendar.readonly'
#search=oauth.get('https://api.yelp.com/v3/businesses/search',params=params)
#results=json.loads(search.content)
#print results['total']

def yelp_search(url,client_id,client_secret,params):
    google = OAuth2Session(client_id, scope=scope, redirect_uri=redirect_uri)
    client=BackendApplicationClient(client_id=client_id)
    authorization_url, state = google.authorization_url(authorization_base_url,
        access_type="offline", approval_prompt="force")
    print 'Please go here and authorize,', authorization_url
    redirect_response = raw_input('Paste the full redirect URL here:')
    oauth=OAuth2Session(client=client)
    google.fetch_token(token_url, client_secret=client_secret,
        authorization_response=redirect_response)
    search=oauth.get(url,params=params)
    content=json.loads(search.content)
    pprint(content)
#    while not total or (fetched<total and fetched<1000):
#        if fetched>0:
#            params['offset']=fetched
#        search=oauth.get(url,params=params)
#        content=json.loads(search.content)
#        try:
#            total=content['total']
#            print total
#        except:
#            pprint(content)
#        results=results+content['businesses']
#        fetched+=40
#        print fetched
#    return results

if __name__ == "__main__":
    yelp_search(url,client_id,client_secret,params)
#    for address in addresses:
#        address_short=address.split(',')[0].replace(" ","")
#        outfile="/Users/nikhil/Documents/businesses_restaurant/"+address_short+".csv"
#        print "pulling data for " + outfile
#        params['location']=address
#        businesses=yelp_search(url,client_id,client_secret,params)
#        flat=flatten_data(businesses,keys)
#        write_to_csv(flat,outfile,header,0)
