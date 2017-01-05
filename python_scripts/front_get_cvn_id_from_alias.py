import requests
import json
from pprint import pprint
import unicodecsv as csv
from datetime import datetime
from time import sleep
import sys
header={"Authorization":"Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzY29wZXMiOlsiKiJdLCJpc3MiOiJmcm9udCIsInN1YiI6ImVhdHNhIn0.ODkHPWeXk5nCve7JNGcVVITtQoRaZdl8ussK4vh7WlY","Accept":"application/json" }

def getData(url,header):
        if header:
            response=requests.get(url,headers=header)
        else:
            response=requests.get(url)
        data=response.content
	jData=json.loads(data)
        return jData

def getFromDict(dataDict, mapList):
            return reduce(lambda d, k: d[k], mapList, dataDict)
def flattenData(apiData,keys):
	flat=[]
	results=apiData['_results']
	for result in results:
		row=[]
		for key in keys:
			try:
				row.append(getFromDict(result,key))
			except:
				row.append('NULL')
                flat.append(row)
        return flat

def writeToCsv(flat_file,mapList,file,colnames):

        with open(file,"w") as a:
                writer=csv.writer(a)
                writer.writerow(colnames)
                for row in flat_file:
			if row[2] is not None:
				writer.writerow(row)


if __name__ == "__main__":
	start_time=datetime.now()
	print datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
	count=1
	final_count=1
	url='https://api2.frontapp.com/conversations/alt:ref:'+sys.argv[1]
	data=getData(url,header)
        print data['id'] 
        with open ("cnv.json","w") as out:
            json.dump(data,out, indent=4)	
        #colNames=['emitted_at','type','event_id','reciepient_email','conversation_id','event_type','meta_id','meta_name','message_blurb']
#	keysNeeded=[['emitted_at']
#			,['type']
#			,['id']
#			,['conversation','recipient','handle']
#			,['conversation','id']
#			,['target','_meta','type']
#			,['target','data','id']
#			,['target','data','name']
#			,['target','data','blurb']]
#	flattened=flattenData(data,keysNeeded)
##	writeToCsv(flattened,keysNeeded,"fa_data/event_data5.csv")
#	while True:
#		print datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
#		file="fa_data/frontapp_event_data_final.csv"
#		new_url=data["_pagination"]["next"]
#		current_time=datetime.now()
#		print int((current_time-start_time).total_seconds())
#		if final_count>=500 or new_url is None:
#			writeToCsv(flattened,keysNeeded,file,colNames)
#			break
#		if int((current_time-start_time).total_seconds())<=60 and count>120:
#			wait=60-int((current_time-start_time).total_seconds())
#			print "sleeping for " + str(wait) +" seconds"	
#			sleep(wait)
#			start_time=datetime.now()
#			count=0
#		data=getData(new_url,header)
#		flattened=flattened+flattenData(data,keysNeeded)
#		count+=1
#		final_count+=1
