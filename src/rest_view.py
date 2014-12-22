#!/usr/bin/python

from __future__ import print_function
from flask import Flask, jsonify, make_response, abort, request
import json
from QueueConsumer import QueueConsumer
from MarshallingAgent import MarshallingAgent
import fes_controller
from FesException import FesException

APP = Flask(__name__)

@APP.route('/add/<string:id_>/<int:expiration>', methods=['POST'])
def add(id_, expiration):
    try:
        event = fes_controller.add(id_, expiration, json.dumps(request.json))
        return jsonify(event.__dict__), 201
    except FesException as e:
        return jsonify({"error" : e.value}), 400

@APP.route('/update/expiration/<string:id_>/<int:expiration>', methods=['PUT'])
def update_expiration(id_, expiration):
    try:
        fes_controller.update_expiration(id_, expiration)
        return jsonify({}), 200
    except FesException as e:
        return jsonify({"error" : e.value}), 400

@APP.route('/update/event/<string:id_>', methods=['PUT'])
def update_event(id_):
    if request.json is None:
        abort(400)

    fes_controller.update_event_payload(id_, json.dumps(request.json))
    return jsonify({}), 200

@APP.route('/delete/<string:id_>', methods=['DELETE'])
def delete(id_):
    fes_controller.delete(id_)
    return jsonify({}), 200

@APP.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

@APP.errorhandler(400)
def bad_request(error):
    return make_response(jsonify({'error': error}), 400)

if __name__ == '__main__':
    #run the daemon that fires events from redis
    print("starting QueueConsumer")
    QUEUE_CONSUMER = QueueConsumer()
    QUEUE_CONSUMER.start()

    #['1_', '2_', ... 'f_']
    SCANNER_PREFIXES = ['%x' % x + '_' for x in xrange(0, 16)]

    #run the daemons that move events from hbase into redis
    for start_row in SCANNER_PREFIXES:
        print("Starting MarshallingAgent start_row=" + start_row)
        MarshallingAgent(start_row).start()

    APP.run(debug=True, use_reloader=False)
