import requests
import json
from pprint import pprint
import unicodecsv as csv
from datetime import datetime
from time import sleep

with open('../config.json') as config:
    conf=json.load(config)
frontapp=conf['frontapp']
header=frontapp['header']



def getDataFromFront(timestamp,filePath):

	def getData(url,header):
	        response=requests.get(url,headers=header)
	        data=response.content
		jData=json.loads(data)
	        return jData
	
	def getFromDict(dataDict, mapList):
	            return reduce(lambda d, k: d[k], mapList, dataDict)
	
	
	def flattenData(apiData,listKey,keys):
	        flat=[]
	        results=apiData[listKey]
	        for result in results:
			row=[]
	        	try:
			        fromto=getFromDict(result,['target','data','recipients'])
				for i in fromto:
					result[i['role']]=i['handle']
	                except (KeyError,TypeError):
				pass
			for key in keys:
	                        try:
	                                row.append(getFromDict(result,key))
	                        except:
	                                row.append('NULL')
	                flat.append(row)
	        return flat
	
	def writeToCsv(flatFile,file,colNames):
	
	        with open(file,"w") as a:
	                writer=csv.writer(a)
	                writer.writerow(colNames)
	                for row in flatFile:
				if row[4] is not None:
					writer.writerow(row)
	

#defaut variables
	start_time=datetime.now()
	if timestamp is None:
		url='https://api2.frontapp.com/events?'
	else:
		url='https://api2.frontapp.com/events?&q[after]='+str(timestamp)
	count=1
	flattened=[]
	col_names=['from','to','emitted_at','type','event_id','reciepient_email','conversation_id','event_type','meta_id','meta_name','message_blurb']
        keys_needed=[['from']
                        ,['to']
                        ,['emitted_at']
                        ,['type']
                        ,['id']
                        ,['conversation','recipient','handle']
                        ,['conversation','id']
                        ,['target','_meta','type']
                        ,['target','data','id']
                        ,['target','data','name']
                        ,['target','data','blurb']]
#loops through the pages
	while True:
		current_time=datetime.now()
#pauses if it reached the front api limit
		if int((current_time-start_time).total_seconds())<=60 and count>120:
                        wait=60-int((current_time-start_time).total_seconds())
                        print "sleeping for " + str(wait) +" seconds"
                        sleep(wait)
                        start_time=datetime.now()
                        count=0
		print datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
		data=getData(url,header)
		flattened=flattened+flattenData(data,'_results',keys_needed)
		count+=1
		url=data["_pagination"]["next"]
		print url
#prints and ends on last page
		if  url is None:
                        print "All Done"
                        writeToCsv(flattened,filePath,col_names)
                        break
	
if __name__ == "__main__":
	getDataFromFront(None,"all_Events.csv")
