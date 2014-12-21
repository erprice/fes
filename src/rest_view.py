#!/usr/bin/python

from __future__ import print_function
from flask import Flask, jsonify, make_response, abort, request
import json
import redis_data
from queue_consumer import queue_consumer
from marshalling_agent import marshalling_agent
import fes_controller
from FesException import FesException

app = Flask(__name__)

@app.route('/add/<string:id_>/<int:expiration>', methods=['POST'])
def add(id_, expiration):
    try:
        event = fes_controller.add(id_, expiration, json.dumps(request.json))
        return jsonify(event.__dict__), 201
    except FesException as e:
        return jsonify({"error" : e.value}), 400

@app.route('/update/expiration/<string:id_>/<int:expiration>', methods=['PUT'])
def update_expiration(id_, expiration):
    try:
        fes_controller.update_expiration(id_, expiration)
        return jsonify({}), 200
    except FesException as e:
        return jsonify({"error" : e.value}), 400

@app.route('/update/event/<string:id_>', methods=['PUT'])
def update_event(id_):
    if request.json is None:
        abort(400)

    fes_controller.update_event_payload(id_, json.dumps(request.json))
    return jsonify({}), 200

@app.route('/delete/<string:id_>', methods=['DELETE'])
def delete(id_):
    fes_controller.delete(id_)
    return jsonify({}), 200

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

@app.errorhandler(400)
def bad_request(error):
    return make_response(jsonify({'error': error}), 400)

if __name__ == '__main__':
    #run the daemon that fires events from redis
    print("starting queue_consumer")
    queue_consumer = queue_consumer()
    queue_consumer.start()

    #['1_', '2_', ... 'f_']
    scanner_prefixes = ['%x' % x + '_' for x in xrange(0,16)]

    #run the daemons that move events from hbase into redis
    for start_row in scanner_prefixes:
        print("Starting marshalling_agent start_row=" + start_row)
        marshalling_agent(start_row).start()

    app.run(debug=True, use_reloader=False)
