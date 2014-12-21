#!/usr/bin/python

from __future__ import print_function
from flask import Flask, jsonify, make_response, abort, request
import datetime

app = Flask(__name__)

@app.route('/expiration', methods=['PUT'])
def add():
    print("Event expired at " + datetime.datetime.utcnow().strftime("%c"))
    if request.json == None:
        print("\tevent=null")
    else:
        print("\tevent=" + str(request.json))
    return "", 200

if __name__ == '__main__':
    app.run(port=5001, debug=True)