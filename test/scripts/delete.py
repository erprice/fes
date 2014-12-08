#!/usr/bin/python

import requests
import sys

url = "http://localhost:5000/delete/" + sys.argv[1]

response = requests.delete(url, headers={'Content-Type': 'application/json'})

print response.text