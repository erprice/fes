#!/usr/bin/python

import requests
import calendar
import datetime
import json
import sys

future = calendar.timegm(datetime.datetime.utcnow().utctimetuple()) + int(sys.argv[1])
url = "http://localhost:5000/add/asdf/" + str(future)

response = requests.post(url, data=json.dumps({"a list" : ["a list item"], "a string" : "string value"}), 
	headers={'Content-Type': 'application/json'})

print response.text