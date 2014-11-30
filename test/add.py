#!/usr/bin/python

import requests
import calendar
import datetime
import json
import sys

future = calendar.timegm(datetime.datetime.utcnow().utctimetuple()) + int(sys.argv[2])
url = "http://localhost:5000/add/" + sys.argv[1] + "/" + str(future)

response = requests.post(url, data=json.dumps({"a list" : ["a list item"], "a string" : "string value"}), 
	headers={'Content-Type': 'application/json'})

print response.text