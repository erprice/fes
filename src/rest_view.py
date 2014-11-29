#!/usr/bin/python

from flask import Flask, jsonify, make_response, abort, request
import datetime
import calendar
import hashlib
import requests
import json
import hbase_data
import redis_data
from future_event import future_event
from queue_consumer import queue_consumer
import fes_controller

app = Flask(__name__)

@app.route('/add/<string:id>/<int:expiration>', methods=['POST'])
def add(id, expiration):
    now = datetime.datetime.utcnow()

    if request.json is None or expiration <= calendar.timegm(now.utctimetuple()):
        abort(400)
    event = future_event(id, request.json, expiration)

    id_hash = fes_controller.generate_hash(id)

    fifteen_minutes_from_now = now + datetime.timedelta(minutes=15)

    if expiration <= calendar.timegm(fifteen_minutes_from_now.utctimetuple()):
        redis_data.add(id_hash, expiration, json.dumps(event.payload))
    else:
        hbase_data.write_hbase_event(id_hash, expiration, json.dumps(event.payload))
        hbase_data.write_hbase_expiration_index(id_hash, expiration)

    return jsonify(event.__dict__), 201

@app.route('/update/expiration/<string:id>/<int:expiration>', methods=['PUT'])
def update_expiration(id, expiration):
    now = datetime.datetime.utcnow()
    if expiration <= calendar.timegm(now.utctimetuple()):
            abort(400)

    id_hash = fes_controller.generate_hash(id)

    fifteen_minutes_from_now = now + datetime.timedelta(minutes=15)

    #redis is faster, so check it first
    redis_score = redis_data.get_expiration(id_hash)

    #it's already in redis
    if redis_score is not None:
        #the new expiration is within 15 minutes, just update redis expiration
        if expiration <= calendar.timegm(fifteen_minutes_from_now.utctimetuple()):
            redis_data.update_expiration(id_hash, expiration)
            return jsonify({}), 200
        else:
            fes_controller.move_event_to_hbase(id_hash, expiration)
            return jsonify({}), 200

    #retrieve event from hbase
    event = hbase_data.read_hbase_event(id_hash)

    #it's in hbase
    if event is not None:
        #the new expiration is not within 15 minutes, just update hbase expiration
        if expiration > calendar.timegm(fifteen_minutes_from_now.utctimetuple()):
            hbase_data.delete_hbase_expiration(id_hash, event.expiration)
            hbase_data.write_hbase_expiration_index(id_hash, expiration)
            hbase_data.write_hbase_event(id_hash, expiration, event.payload)
            return jsonify({}), 200
        else:
            fes_controller.move_event_to_redis(id_hash, event.expiration)
            return jsonify({}), 200
    
    return jsonify({}), 400

@app.route('/update/event/<string:id>', methods=['PUT'])
def update_event(id):
    if request.json is None:
            abort(400)

    redis_data.update_event(fes_controller.generate_hash(id), json.dumps(request.json))
    return jsonify({}), 200

@app.route('/delete/<string:id>', methods=['DELETE'])
def delete(id):
    id_hash = fes_controller.generate_hash(id)
    redis_data.delete(id_hash)
    return jsonify({}), 200

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

@app.errorhandler(400)
def not_found(error):
    return make_response(jsonify({'error': 'Malformed request'}), 400)

if __name__ == '__main__':
    queueConsumer = queue_consumer()
    queueConsumer.start()
    app.run(debug=True)
