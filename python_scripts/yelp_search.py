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

client_id="LtsNT-6doO-eV-aTZRVFfA"
client_secret="A1CzqjwIJLllMtbHESfvsGFIPJxqVSjHAZviyKVZfGYycpIGl31G7Csrm8ngC94o"
#client=BackendApplicationClient(client_id=client_id)
#oauth=OAuth2Session(client=client)
#token=oauth.fetch_token(token_url='https://api.yelp.com/oauth2/token',client_id=client_id,client_secret=client_secret)

#reviews=oauth.get('https://api.yelp.com/v3/businesses/eatsa-san-francisco-2/reviews')
#pprint(reviews.content)

params={
"location":"285 Madison Ave, New York, NY 10017",
"radius":483,
"limit":40
}
url='https://api.yelp.com/v3/businesses/search'
#search=oauth.get('https://api.yelp.com/v3/businesses/search',params=params)
#results=json.loads(search.content)
#print results['total']

def yelp_search(ull,client_id,client_secret,params):
    client=BackendApplicationClient(client_id=client_id)
    oauth=OAuth2Session(client=client)
    token=oauth.fetch_token(token_url='https://api.yelp.com/oauth2/token',client_id=client_id,client_secret=client_secret) 
    fetched=0
    results=[]
    total=None
    while not total or fetched<total:
        search=oauth.get(url,params=params)
        content=json.loads(search.content)
        try:
            total=content['total']
        except:
            pprint(content)
        results=results+content['businesses']
        fetched+=40
        print fetched
    return results

keys=[['name'],
['location','address1']
,['location','address2']
,['location','address3']
,['location','city']
,['location','state']
,['location','zip_code']
,['coordinates','latitude'],['coordinates','longitude']
,['distance']
,['rating']
,['review_count']
,['url'],['price']]

addresses=[
'121 Spear St, San Francisco, CA 94105',
'1 California St, San Francisco, CA 94111',
'285 Madison Ave, New York, NY 10017',
'666 3rd Ave, New York, NY 10172',
'1701 Pennsylvania Ave NW, Washington, DC 20006',
'1627 K St NW, Washington, DC 20006',
'30 N LaSalle St, Chicago, IL 60602',
'164 Pearl St, New York, NY 10005',
'115 5th Ave, New York, NY 10003',
'1964 3rd Ave, New York, NY 10029',
'141 Livingston St, Brooklyn, NY 11201',
'680 6th Ave, New York, NY 10010',
'12 Vesey St, New York, NY 10007',
'810 7th Ave, New York, NY 10019',
'472 Broome St, New York, NY 10012',
'460 Park Ave S, New York, NY 10016',
'1267 F St NW, Washington, DC 20004',
'1345 Connecticut Ave NW, Washington, DC 20036',
'727 5th St NW, Washington, DC 20001',
'1217 Massachusetts Ave NW, Washington, DC 20005',
'633 D St NW, Washington, DC 20004',
'1999 K St NW, Washington, DC 20006',
'701 Market St, San Francisco, CA 94103',
'680 Mission St, San Francisco, CA 94105',
'200 Kansas St, San Francisco, CA 94103',
'900 North Point St, San Francisco, CA 94109',
'652 Polk St, San Francisco, CA 94102',
'599 3rd St, San Francisco, CA 94107',
'505 Parnassus Ave, San Francisco, CA 94143']
header=['name','address line 1','address line 2', 'address line 3','city','state','zip code','latitude','longitude','distance','rating','review_count','url','price']
if __name__ == "__main__":
    for address in addresses:
        address_short=address.split(',')[0].replace(" ","")
        outfile="/Users/nikhil/Documents/businesess/"+address_short+".csv"
        print "pulling data for " + outfile
        params['location']=address
        businesses=yelp_search(url,client_id,client_secret,params)
        flat=flatten_data(businesses,keys)
        write_to_csv(flat,outfile,header,0)
