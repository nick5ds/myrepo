import requests
import json
from pprint import pprint
import unicodecsv as csv
from datetime import datetime
from time import sleep
header = {
    "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzY29wZXMiOlsiKiJdLCJpc3MiOiJmcm9udCIsInN1YiI6ImVhdHNhIn0.ODkHPWeXk5nCve7JNGcVVITtQoRaZdl8ussK4vh7WlY",
    "Accepti": "application/json"}


def get_data(url, header):
    response = requests.get(url, headers=header)
    jData = response.json()
    return jData


def check_rate(start_time, req_count):
    """ checks the rate at which the API is being called to deal with Front's limits"""
    current_time = datetime.now()
    print("current time interval " + str((current_time -
                                          start_time).total_seconds()) + " current count " + str(req_count))
    if int((current_time - start_time).total_seconds()) <= 60 and req_count > 120:
        wait = 60 - int((current_time - start_time).total_seconds())
        print("sleeping for " + str(wait) + " seconds")
        sleep(wait)
        start_time = datetime.now()
        return True
    elif int((current_time - start_time).total_seconds()) >= 60:
        return True
    else:
        return False


def get_paginated_data(url, header):
    """  to make this generalized, the key for the results array along with the 
    location of the next page url would need to be variables """
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
        results += data['_results']
        url = data["_pagination"]["next"]
        if not url:
            break
        count += 1
        did_pause = check_rate(start_time, count)
        if did_pause:
            count = 0
            start_time = datetime.now()
    return results


def get_from_dict(data_dict, map_list):
    """returns elements from a nested dictionary using a list of keys"""
    if not isinstance(map_list,list):
        raise TypeError(str(map_list) +" is not a list")
    for i in map_list:
        data_dict = data_dict[i]
    return data_dict

def flatten_data(api_data, keys, list_key=None):
    """ Traverses  a list of nested dictionaries and returns a list of key values.
        
        keys should be a list of lists. Each element within the lists represents a level within the dictionary.
        i.e. [[recipient,handle]] returns the value of handle which is nested within recipient

        if the list that that needs to be flatted is nested somewhere within a dictionaru
        pass in the list of keys to reach the element"""
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

def explode_tags(data_array, tag_index):
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

def create_primary_key(flat_list,index_list):
    """ concatenates columns in order to create a primary key"""
    for row in flat_list:
        pkey=''.join([str(row[i]) for i in index_list])
        row.append(pkey)
    return flat_list

def write_to_csv(flat_file, file_, colNames,pkey):
    """ wirtes data to csv if the pkey column in not null"""
    with open(file_, "wb") as a:
        writer = csv.writer(a)
        writer.writerow(col_names)
        for row in flat_file:
            if row[pkey]:
                writer.writerow(row)



if __name__ == "__main__":
    cnv = get_paginated_data('https://api2.frontapp.com/conversations?',header)
    keys = [['recipient', 'handle'], ['assignee', 'email'], ['id'],
            ['subject'], ['last_message', 'created_at'], ['tags']]
    flat = flatten_data(cnv, keys)
    exploded = explode_tags(flat, 5)
    final=create_primary_key(exploded,[2,5])
    col_names = ["from_email", "to_email", "conversation_id",
                 "subject", "last_message_timestamp", "tag_id", "tag_name","pkey"]
    write_to_csv(final, "tags.csv", col_names,7)
