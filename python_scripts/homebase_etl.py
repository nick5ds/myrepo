import requests
import json
from pprint import pprint
import unicodecsv as csv
import datetime 
from time import sleep




def _get_start_end_times(date, epoch=False):
    '''
    get start/end times in either ISO8601 or epoch
    '''
    # convert to datetime object
    date_obj_end = datetime.datetime.strptime(date, '%Y-%m-%d')
    date_obj_start = date_obj_end - datetime.timedelta(seconds=date_interval)

    # convert to epoch timestamp integer
    epoch_start = int((date_obj_start - datetime.datetime(1970, 1, 1)).total_seconds())
    epoch_end = int((date_obj_end - datetime.datetime(1970, 1, 1)).total_seconds())

    # convert to ISO8601 string
    iso8601_start = datetime.datetime.strftime(date_obj_start, '%Y-%m-%d')
    iso8601_end = datetime.datetime.strftime(date_obj_end, '%Y-%m-%d')

    if epoch:
        return epoch_start, epoch_end
    else:
        return iso8601_start, iso8601_end



def get_homebase_timecard_and_timebreak_data(**kwargs):
    '''
    main homebase etl
    '''
    start_date ,end_date = _get_start_end_times(datetime.datetime.now().strftime('%Y-%m-%d'))
    data_raw = get_paginated_data(timecards_url,header,results_list,start_date,end_date)
    #pprint(cnv)
    timecards = flatten_data(data_raw, timecard_keys)
    time_break=explode_rows(timecards,0,timebreak_keys)
#    delete_column(timecards,0)
    write_to_csv(timecards, timecard_write_to_file_directory, timecard_col_names,0)
    write_to_csv(time_break, timebreak_write_to_file_directory,timebreak_col_names,0) 

def get_homebase_shifts_data(**kwargs):
    '''
     main homebase etl
    '''
    data_raw=get_paginated_data(shifts_url,header,results_list,start_date,end_date)
    shifts=flatten_data(data_raw,shifts_keys)
    write_to_csv(shifts,shifts_dir,shift_col_names,0)

def delete_column(data,index):
    for i in data:   
        del i[index]


def get_data_with_header(url, header):
    response = requests.get(url, headers=header)
    response_header=response.headers
    jdata = response.json()
    final_data={"header":response_header,"body":jdata}
    return final_data

def get_paginated_data(url, header,results_list,
                        start_date,end_date):
    original_url=url+"&start_date="+str(start_date)+"&end_date="+str(end_date)
    results = []
    start_time = datetime.datetime.now()
    count = 1
    while True:
        url=original_url+"&page="+str(count) 
        print(
            datetime.datetime.strftime(
                datetime.datetime.now(),
                "%Y-%m-%d %H:%M:%S") +
            " " +
            url)
        data = get_data_with_header(url, header)
        result=get_from_dict(data,results_list)
        if not result: 
            break
        results += get_from_dict(data,results_list)
        count += 1
    return results


def get_from_dict(data_dict, map_list):
    """returns elements from a nested dictionary using a list of keys"""
    if not isinstance(map_list,list):
        raise TypeError(str(map_list) +" is not a list")
    for i in map_list:
        data_dict = data_dict[i]
    return data_dict

def flatten_data(api_data, keys, list_key=None):
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

def explode_rows(data_array, tag_index,flat_keys):
    """ this is front specific and explodes the list of dictionries that 
        contains the tags in each conversation"""
    explode = []
    for arr in data_array:
        tags = arr.pop(tag_index)
        if tags: 
            exprow = flatten_data(tags,flat_keys)
            for elem in exprow:
                explode.append(elem)
    return explode

def create_primary_key(flat_list,index_list):
    """ concatenates list values (specified by a list of idexes)
         in order to create a primary key"""
    for row in flat_list:
        pkey=''.join([str(row[i]) for i in index_list])
        row.append(pkey)
    return flat_list

def write_to_csv(flat_file, file_, col_names,pkey):
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
    frontapp=conf['homebase']
    header=frontapp['header']    
    timecards_url='https://api.joinhomebase.com/locations/23d67ee6-a64f-456f-bb0e-eb186ceee20b/timecards?per_page=100'
    shifts_url='https://api.joinhomebase.com/locations/23d67ee6-a64f-456f-bb0e-eb186ceee20b/shifts?per_page=100'
    timecard_keys = [
            ['timebreaks'],
            ['id'], ['user_id'], ['first_name'],
            ['last_name'], ['job_id'], ['shift_id'],['role'],['department'],
            ['created_at'],['updated_at'],['clock_in'],['clock_out']
            ,['labor','break_penalty'],['labor','costs'],['labor','weekly_overtime'],['labor','paid_time_off_hours'],
            ['labor','time_off_hours'],['labor','unpaid_break_hours'],['labor','regular_hours'],['labor', 'paid_hours'],
            ['labor', 'scheduled_hours'],['labor', 'daily_overtime'], ['labor','double_overtime'], ['labor', 'seventh_day_overtime_15'], 
            ['labor','seventh_day_overtime_20'], ['labor','wage_rate'],['labor','wage_type'],['approved']
            ]
    tag_index=5
    make_pkey_indexes=[2,5]
    timecard_col_names = [
               "id","user_id","first_name","last_name","job_id","shift_id","role","department","created_at","updated_at","clock_in","clock_out",
                "break_penalty","labor_costs","weekly_overtime","pto_hours","time_off_hours","unpaid_break_hours","regular_hours","paid_hours","scheduled_hours",
                "daily_overtime","double_overtime","seventh_day_overtime_15","seventh_day_overtime_20","wage_rate","wage_type","is_approved" 
                ]
    
    timebreak_keys=[
        ['id'],['timecard_id'],['paid'],['duration'],['work_period'],['created_at'],['updated_at'],['start_at'],['end_at']
    ]

    shifts_keys=[
        ['id'], ['timecard_id'], ['open'], ['role'], ['department'], ['first_name'], ['last_name'], ['job_id'], ['user_id'],
        ['wage_rate'], ['published'], ['scheduled'], ['created_at'], ['updated_at'], ['start_at'], ['end_at']
    ]
    shift_col_names=['id', 'timedcard_id', 'is_open', 'role', 'department', 'first_name', 'last_name', 'job_id', 'user_id', 'wage_rate', 
            'published', 'scheduled', 'created_at', 'updated_at', 'start_at', 'end_at']
    timebreak_col_names=['id','timecard_id','is_paid','duration','work_period','created_at','updated_at','start_at','end_at']
    results_list=['body']
    timecard_write_to_file_directory="/Users/nikhil/data_work/time_cards.csv"
    timebreak_write_to_file_directory="/Users/nikhil/data_work/time_break.csv"
    shifts_dir="/Users/nikhil/data_work/shifts.csv"
#    start_date='2017-02-02'
#    end_date='2017-02-03'
    date_interval=2592000
    get_homebase_timecard_and_timebreak_data()    
#    get_homebase_shifts_data()
#    print _get_start_end_times(datetime.datetime.now().strftime('%Y-%m-%d'))
    #pkey_index=7
    #cnv = get_paginated_data(cnv_url,header,['body'],'2016-12-01','2016-12-03')
    #pprint(cnv)
    #timecards = flatten_data(cnv, timecard_keys)
    #time_break=explode_tags(timecards,0,timebreak_keys)
    #for i in timecards:
    #    del i[0]


    #exploded = explode_tags(flat, tag_index)
    #final=create_primary_key(exploded,make_pkey_indexes)
    #write_to_csv(timecards, timecard_write_to_file_directory, timecard_col_names,0)
    #write_to_csv(time_break, timebreak_write_to_file_directory,timebreak_col_names,0)

