import requests
import json
from pprint import pprint
import unicodecsv as csv
from datetime import datetime
from time import sleep
header={"Authorization":"Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzY29wZXMiOlsiKiJdLCJpc3MiOiJmcm9udCIsInN1YiI6ImVhdHNhIn0.ODkHPWeXk5nCve7JNGcVVITtQoRaZdl8ussK4vh7WlY","Accepti":"application/json" }




def getData(url,header):
    response=requests.get(url,headers=header)
    data=response.text
    jData=json.loads(data)
    return jData


def checkRate(startTime,Count):
    current_time=datetime.now()
    if int((current_time-startTime).total_seconds())<=60 and Count>120:
        wait=60-int((current_time-start_time).total_seconds())
        print( "sleeping for " + str(wait) +" seconds")
        sleep(wait)
        start_time=datetime.now()
        return True
    else:
        return False

def getPaginatedData(url,header):
    results=[]
    start_time=datetime.now()
    count=0
    while True:
        print(datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")+" "+url)
        data=getData(url,header)            
        results+=data['_results']
        url=data["_pagination"]["next"]
        if url is None:
            break
        count+=1
        did_pause=checkRate(start_time,count)
        if did_pause==True:
            count=0
            start_time=datetime.now()
    return results

def getFromDict(dataDict, mapList):
        for i in mapList:
            dataDict=dataDict[i]
        return dataDict

def flattenData(apiData,keys,listKey=None):
        flat=[]
        if listKey is not None:
            results=apiData[listKey]
        else:
            results=apiData
        for result in results:
            row=[]
            for key in keys:
                try:
                    row.append(getFromDict(result,key))
                except:
                    row.append('NULL')
            flat.append(row)
        return flat

def explodeTags(dataArray,tagIndex):
    explode=[]
    for arr in dataArray:
        tags=arr.pop(tagIndex)
        for tag in tags:
            exprow=[]
            exprow += arr
            exprow.append(tag['id'])
            exprow.append(tag['name'])
            explode.append(exprow)
            pprint(exprow)
    return explode
def writeToCsv(flatFile,file,colNames):

     with open(file,"wb") as a:
        writer=csv.writer(a)
        writer.writerow(colNames)
        for row in flatFile:
            if row[2] is not None:
                writer.writerow(row)

def getConversationList(lastUpdate=None):
    if lastUpdate is None:
        url='https://api2.frontapp.com/conversations?'
    else:
        url='https://api2.frontapp.com/conversations/?q[after]='+str(lastUpdate)
    print(url)
    conversations=getData(url,header) 
    return conversations


def getEventList(lastUpdate=None,filePath):
#defaut variables
    start_time=datetime.now()
    if lastUpdate is None:
        url='https://api2.frontapp.com/events?'
    else:
        url='https://api2.frontapp.com/events?&q[after]='+str(lastUpdate)
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
    events=getPaginatedData(url,header)
    return events
#prints and ends on last page
    
if __name__ == "__main__":
    cnv=getConversationList(1473622549)
#    pprint(cnv)
    keys=[['recipient','handle'],['assignee','email'],['id'],['subject'],['last_message','created_at'],['tags']]
    flat=flattenData(cnv,keys,"_results")
#    pprint(flat)
    exploded=explodeTags(flat,5)
    col_names=["from_email","to_email","conversation_id","subject","last_message_timestamp",,"tag_name","tag_id"]
    writeToCsv(exploded,"cnvlist.csv",col_names)
    #pprint(jData)
#    getDataFromFront(None,"all_Events.csv")
