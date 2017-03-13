from pprint import pprint
import requests

body={'body': [{u'created_at': u'2017-01-24T10:08:46-08:00',
           u'email': u'tim@eatsa.com',
           u'first_name': u'Tim',
           u'id': 1344317,
           u'job': {u'archived_at': None,
                    u'id': 1083014,
                    u'payroll_id': None,
                    u'pin': u'118061',
                    u'role': u'General Manager',
                    u'roles': [],
                    u'wage_rate': None,
                    u'wage_type': u'hourly'},
           u'last_name': u'Young',
           u'phone': None,
           u'updated_at': u'2017-01-28T13:40:18-08:00'}],
 'header': {'Status': '200 OK', 'X-Request-Id': '651d6a5a-6dc6-4cdd-881c-1ca792a6af44', 'Strict-Transport-Security': 'max-age=31536000', 'Vary': 'Origin', 'Content-Length': '321', 'Server': 'nginx/1.8.1', 'Connection': 'keep-alive', 'X-Runtime': '0.165543', 'ETag': '"ac472f61482441a8d0759fe69bb31d42"', 'Link': '<https://joinhomebase.com/api/public/locations/23d67ee6-a64f-456f-bb0e-eb186ceee20b/employees?end_date=2016-12-05&group_by=day&page=48&per_page=1&start_date=2016-12-01>; rel="last", <https://joinhomebase.com/api/public/locations/23d67ee6-a64f-456f-bb0e-eb186ceee20b/employees?end_date=2016-12-05&group_by=day&page=2&per_page=1&start_date=2016-12-01>; rel="next"', 'Cache-Control': 'must-revalidate, private, max-age=0', 'Date': 'Thu, 02 Feb 2017 22:31:18 GMT', 'Total': '48', 'Content-Type': 'application/json', 'X-Rack-Cache': 'miss', 'Per-Page': '1'}}

link=body['header']['Link']

def parse_link(link):
    nex_link=link.split(',')[1].split(';')[0].replace('<','').replace('>','')
    print next_link


print requests.utils.parse_header_links(link)
