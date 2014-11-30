#!/usr/bin/python

from flask import Flask, jsonify, make_response, abort, request
import json
import hbase_data
import redis_data
from future_event import future_event
from queue_consumer import queue_consumer
import fes_controller
from FesException import FesException

app = Flask(__name__)

@app.route('/add/<string:id>/<int:expiration>', methods=['POST'])
def add(id, expiration):
    try:
        event = fes_controller.add(id, expiration, json.dumps(request.json))
        return jsonify(event.__dict__), 201
    except FesException as e:
        return jsonify({"error" : e.value}), 400

@app.route('/update/expiration/<string:id>/<int:expiration>', methods=['PUT'])
def update_expiration(id, expiration):
    try:
        fes_controller.update_expiration(id, expiration)
        return jsonify({}), 200
    except FesException as e:
        return jsonify({"error" : e.value}), 400

@app.route('/update/event/<string:id>', methods=['PUT'])
def update_event(id):
    if request.json is None:
            abort(400)

    redis_data.update_event(fes_controller.generate_hash(id), json.dumps(request.json))
    return jsonify({}), 200

@app.route('/delete/<string:id>', methods=['DELETE'])
def delete(id):
    fes_controller.delete(id)
    return jsonify({}), 200

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

@app.errorhandler(400)
def not_found(error):
    return make_response(jsonify({'error': error}), 400)

if __name__ == '__main__':
    queueConsumer = queue_consumer()
    queueConsumer.start()
    app.run(debug=True)
