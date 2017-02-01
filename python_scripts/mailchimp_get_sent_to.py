import requests
import json
from pprint import pprint
import csv
with open('../config.json') as config:
    conf=json.load(config)
mailchimp=conf['mailchimp']


auth=('anystring',mailchimp)


#url='https://us9.api.mailchimp.com/3.0/reports/b3239a9fcc/sent-to'
#payload={'fields':'sent_to.email_id,sent_to.email_address,sent_to.status,sent_to.open_count,sent_to.campaign_id,sent_to.last_open','count':100}
payload={'fields':'emails.activity,emails.email_address,emails.campaign_id','count':10000}
#payload={'count':10}
#response=requests.get(url,auth=auth,params=payload)
#print(response.content)
#data=response.content
#print data["id"]

#pruned["id"]=data["id"]
#pruned["type"]=data['type']
#print len(data["_results"])

campaigns=[
'edf775287e'
#'3580537516'
]
out_file=[]

	

def flatten_json(jdata,list_key):
	dataList=jdata[list_key]
	flat=[]
	for row in dataList:
		singleRow=[]
		for key in row:
			singleRow.append(row[key])
		flat.append(singleRow)
	return flat



def writeCsv(flatFile,outfile,header):
	with open(outfile,"w") as a:
		writer=csv.writer(a)
		writer.writerow(header)
		for row in flatFile:
			if row[0] is not None:
				writer.writerow(row)



for c in campaigns:
	url='https://us9.api.mailchimp.com/3.0/reports/'+c+'/email-activity'
	print url
	response=requests.get(url,auth=auth,params=payload)
	data=response.content
	jData=json.loads(data)
	pprint(jData)
	out_file+= flatten_json(jData,'emails')
	pprint(out_file)

writeCsv(out_file,'mailchimp_email_activity.csv',['email_id','status','email','campaign_id','open_count'])



#results=jData['_results']
#for result in results:
#	print result.keys()
#	row=[]
#	row.append(result['emitted_at'])
#	row.append(result['type'])
#	row.append(result['id'])
#	row.append(result['conversation']['recipient']['handle'])
#	row.append(result['conversation']['id'])
##	row.append(result['author']['email'])
#	row.append(result['conversation']['last_message']['id'])
#	row.append(result['conversation']['last_message']['type'])
#	row.append(result['conversation']['last_message']['blurb'])
#	try:
#		row.append(result['target']['_meta']['type'])
#
#	except KeyError:
#		row.append('NULL')
#	try:
#		row.append(result['target']['data']['id'])
#	except KeyError:
#		row.append('NULL')
#	try:
#		row.append(result['target']['data']['name'])
#	except KeyError:
#		row.append('NULL')
#	pruned.append(row)
##pprint(jData)
#header=["emitter_at","type","id","recipient_handle","conversation_id","last_message_id","last_message_type","last_message_blurb","meta_type","meta_name","mata_id"]
#with open("data.csv","w") as a:
#	writer=csv.writer(a)
#	writer.writerow(header)
#	for row in pruned:
#		writer.writerow(row)
#with open("message.json","w") as a:
#		json.dump(jData,a,indent=4)

