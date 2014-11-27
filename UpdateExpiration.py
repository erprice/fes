import requests
import calendar
import datetime
import json
import sys

future = calendar.timegm(datetime.datetime.utcnow().utctimetuple()) + int(sys.argv[2])
url = "http://localhost:5000/update/expiration/" + sys.argv[1] + "/" + str(future)

response = requests.put(url, headers={'Content-Type': 'application/json'})

print response.text