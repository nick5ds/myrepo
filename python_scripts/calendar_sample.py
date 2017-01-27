#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2014 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Simple command-line sample for the Calendar API.
Command-line application that retrieves the list of the user's calendars."""

import sys
sys.path.insert(1, '/Library/Python/2.7/site-packages')
from oauth2client import client
from googleapiclient import sample_tools
from pprint import pprint

def main(argv):
    # Authenticate and construct service.
    service, flags = sample_tools.init(
        argv, 'calendar', 'v3', __doc__, __file__,
        scope='https://www.googleapis.com/auth/calendar.readonly')
    list_out=[]
    try:
        page_token = None
        while True:
            events = service.events().list(calendarId='eatsa.com_baequihu4jrcoo2l328efku5mk@group.calendar.google.com', pageToken=page_token).execute()
            for event in events['items']:
                event_list=[]
                event_list.append(event['summary'])
                list_out.append(event_list)
                pprint(list_out) 
                page_token = events.get('nextPageToken')
            if not page_token:
                return list_out            
    except client.AccessTokenRefreshError:
        print('The credentials have been revoked or expired, please re-run'
              'the application to re-authorize.')

if __name__ == '__main__':
   print  main(sys.argv)
