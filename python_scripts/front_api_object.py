import requests
import json
from pprint import pprint
import unicodecsv as csv
from datetime import datetime
from time import sleep
class apiObject:
    def __init__(self,**kwargs):
        self.url=kwargs.get('url')
        self.header=kwargs.get('header')
        keys=kwargs.get('keys')
        list_key=kwargs.get('list_key')
        col_names=kwargs.get('col_names')
        tag_index=kwargs.get('tag_index')        
        index_list=kwargs.get('pkey_columns')
        file_=kwargs.get('csv_file')
        pkey=kwargs.get('pkey_index')        
        print(self.url)


    print(keys)
    def get_data(self,url, header):
        response = requests.get(url, headers=header)
        jData = response.json()
        return jData
    
    
    def check_rate(self,start_time, req_count,max_req,time_limit):
        """ checks the rate at which the API is being called to deal with 
            imposed limits"""
        current_time = datetime.now()
        print("current time interval " 
                + str((current_time -start_time).total_seconds())
                + " current count " 
                + str(req_count))
        if (int((current_time - start_time).total_seconds()) <= time_limit 
                and req_count > max_req):
            wait = time_limit - int((current_time - start_time).total_seconds())
            print("sleeping for " + str(wait) + " seconds")
            sleep(wait)
            return True
        elif int((current_time - start_time).total_seconds()) >= time_limit:
            return True
        else:
            return False
    
    
    def get_paginated_data(self,url, header,results_list=["_results"],
                            next_url_key=["_pagination","next"],api_limit=True
                            ,max_req=120,time_limit=60):
        results = []
        start_time = datetime.now()
        count = 0
        while True:
            print(
                datetime.strftime(
                    datetime.now(),
                    "%Y-%m-%d %H:%M:%S") +
                " " +
                url)
            data = get_data(url, header)
            results += get_from_dict(data,results_list)
            url = get_from_dict(data,next_url_key)
            if not url:
                break
            count += 1
            if api_limit:
                did_pause = check_rate(start_time, count,max_req,time_limit)
                if did_pause:
                    count = 0
                    start_time = datetime.now()
        return results
    
    
    def get_from_dict(self,data_dict, map_list):
        """returns elements from a nested dictionary using a list of keys"""
        if not isinstance(map_list,list):
            raise TypeError(str(map_list) +" is not a list")
        for i in map_list:
            data_dict = data_dict[i]
        return data_dict
    
    def flatten_data(self,api_data, keys, list_key=None):
        """ Traverses  a list of nested dictionaries and returns a list of values
             for specified keys.
            
            keys should be a list of lists. Each element within the lists 
            represents a level within the dictionary.i.e. 
            [[recipient,handle]] returns the value of handle which is nested
             within recipient
    
            if the list that that needs to be flatted is nested somewhere within 
            a dictionaru pass in the key(s) to reach that list (also in list of
            list form)"""
        flat = []
        if list_key:
            results = get_from_dict(api_data,list_key)
        else:
            results = api_data
        for result in results:
            row = []
            for key in keys:
                try:
                    row.append(get_from_dict(result, key))
                except (KeyError,TypeError):
                    row.append('NULL')
            flat.append(row)
        return flat
    
    def explode_tags(self,data_array, tag_index):
        """ this is front specific and explodes the list of dictionries that 
            contains the tags in each conversation"""
        explode = []
        for arr in data_array:
            tags = arr.pop(tag_index)
            if tags: 
                for tag in tags:
                    exprow = []
                    exprow += arr
                    exprow.append(tag['id'])
                    exprow.append(tag['name'])
                    explode.append(exprow)
        return explode
    
    def create_primary_key(self,flat_list,index_list):
        """ concatenates list values (specified by a list of idexes)
             in order to create a primary key"""
        for row in flat_list:
            pkey=''.join([str(row[i]) for i in index_list])
            row.append(pkey)
        return flat_list
    
    def write_to_csv(self,flat_file, file_, colNames,pkey):
        """ writes data to csv if the pkey column in not null"""
        with open(file_, "wb") as a:
            writer = csv.writer(a)
            writer.writerow(col_names)
            for row in flat_file:
                if row[pkey]:
                    writer.writerow(row)



if __name__ == "__main__":
    with open('../config.json') as config:
        conf=json.load(config)
    frontapp=conf['frontapp']
     
    front=apiObject(
    url='https://api2.frontapp.com/conversations?'
    ,header=frontapp['header']
    ,keys= [
            ['recipient', 'handle'], ['assignee', 'email'], ['id'],
            ['subject'], ['last_message', 'created_at'], ['tags'],['metadata']
            ]
    ,list_key=["_results"]
    ,col_names=[
                "from_email", "to_email", "conversation_id",
                "subject", "last_message_timestamp", "tag_id",
                "tag_name","pkey","meta"
                ]
    ,tag_index=5
    ,pkey_columns=[2,5]
    ,csv_file="/Users/nikhil/data_work/tags.csv"  



    
    ,pkey_index=7
    )
    #front.get_paginated_data() 
    #cnv = get_data(cnv_url,header)
   # flat = flatten_data(cnv, keys,["_results"])
    #exploded = explode_tags(flat, tag_index)
    #final=create_primary_key(exploded,make_pkey_indexes)
    #write_to_csv(final, write_to_file_directory, col_names,pkey_index)
