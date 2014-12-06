#!/usr/bin/python

import requests
import calendar
import datetime
import json
import sys
import time

if len(sys.argv) < 4:
    print "Usage: add.py <key> <num_seconds> <num_events>"
else:
    number = int(sys.argv[3])

    for i in xrange(1, number + 1):
        future = calendar.timegm(datetime.datetime.utcnow().utctimetuple()) + int(sys.argv[2])
        url = "http://localhost:5000/add/" + sys.argv[1] + "_" + str(i) + "/" + str(future)
        response = requests.post(url, data=json.dumps({"a list" : ["a list item"], "a string" : "string value"}), 
    	   headers={'Content-Type': 'application/json'})
        print response.text