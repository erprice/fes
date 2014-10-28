import requests
import calendar
import datetime
import json

five_seconds_away = calendar.timegm(datetime.datetime.utcnow().utctimetuple()) + 5
url = "http://localhost:5000/add/asdf/" + str(five_seconds_away)

response = requests.post(url, data=json.dumps({"a list" : ["a list item"], "a string" : "string value"}), 
	headers={'Content-Type': 'application/json'})

print response.text