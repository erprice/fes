#!/usr/bin/python

from flask import Flask, jsonify, make_response, abort, request
import redis
import datetime
import calendar
import hashlib
import threading
import time
import requests
import json
from hbase_controller import *
from future_event import future_event

app = Flask(__name__)
redis_server = redis.Redis("localhost")

EXPIRATION_QUEUE = "fes:expiration_queue"
EVENT_QUEUE = "fes:event_queue"

FES_CONSUMER_URL = "http://localhost:5001/expiration"

@app.route('/add/<string:id>/<int:expiration>', methods=['POST'])
def add(id, expiration):
    now = datetime.datetime.utcnow()

    if request.json is None or expiration <= calendar.timegm(now.utctimetuple()):
        abort(400)
    event = future_event(id, request.json, expiration)

    id_hash = generate_hash(id)

    fifteen_minutes_from_now = now + datetime.timedelta(minutes=15)

    if expiration <= calendar.timegm(fifteen_minutes_from_now.utctimetuple()):
        pipe = redis_server.pipeline()
        pipe.zadd(EXPIRATION_QUEUE, id_hash, expiration)
        pipe.hset(EVENT_QUEUE, id_hash, json.dumps(event.payload))
        pipe.execute()
    else:
        write_hbase_event(id_hash, expiration, json.dumps(event.payload))
        write_hbase_expiration_index(id_hash, expiration)

    return jsonify(event.__dict__), 201

@app.route('/update/expiration/<string:id>/<int:expiration>', methods=['PUT'])
def update_expiration(id, expiration):
    now = datetime.datetime.utcnow()
    if expiration <= calendar.timegm(now.utctimetuple()):
            abort(400)

    id_hash = generate_hash(id)

    fifteen_minutes_from_now = now + datetime.timedelta(minutes=15)

    #redis is faster, so check it first
    redis_score = redis_server.zscore(EXPIRATION_QUEUE, id_hash)

    #it's already in redis
    if redis_score is not None:
        #the new expiration is within 15 minutes, just update redis expiration
        if expiration <= calendar.timegm(fifteen_minutes_from_now.utctimetuple()):
            redis_server.zadd(EXPIRATION_QUEUE, id_hash, expiration)
            return jsonify({}), 200
        else:
            move_event_to_hbase(id_hash, expiration)
            return jsonify({}), 200

    #retrieve event from hbase
    event = read_hbase_event(id)

    #it's in hbase
    if event is not None:
        #the new expiration is not within 15 minutes, just update hbase expiration
        if expiration > calendar.timegm(fifteen_minutes_from_now.utctimetuple()):
            delete_hbase_expiration(id_hash, event.expiration)
            write_hbase_expiration_index(id_hash, expiration)
            write_hbase_event(id_hash, expiration, event.payload)
            return jsonify({}), 200
        else:
            move_event_to_redis(id, event.expiration)
            return jsonify({}), 200
    
    return jsonify({}), 400

@app.route('/update/event/<string:id>', methods=['PUT'])
def update_event(id):
    if request.json is None:
            abort(400)

    redis_server.hset(EVENT_QUEUE, generate_hash(id), request.json)
    return jsonify({}), 200

@app.route('/delete/<string:id>', methods=['DELETE'])
def delete(id):

    id_hash = generate_hash(id)

    pipe = redis_server.pipeline()
    pipe.zrem(EXPIRATION_QUEUE, id_hash)
    pipe.hdel(EVENT_QUEUE, id_hash)
    pipe.execute()
    
    return jsonify({}), 200

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

@app.errorhandler(400)
def not_found(error):
    return make_response(jsonify({'error': 'Malformed request'}), 400)

def generate_hash(id):
    return hashlib.sha224(id).hexdigest()

def move_event_to_hbase(id_hash, expiration):
    #get event from redis and delete both entries
    pipe = redis_server.pipeline()
    pipe.hget(EVENT_QUEUE, id_hash)
    pipe.zscore(EXPIRATION_QUEUE, id_hash)
    pipe.hdel(EVENT_QUEUE, id_hash)
    pipe.zrem(EXPIRATION_QUEUE, id_hash)
    redis_response = pipe.execute()

    data = redis_response[0]

    #TODO better exception handling
    if data is None:
        print "ERROR copying event " + id_hash + " to hbase, data not found"
        return

    if expiration is None:
        expiration = int(redis_response[1])

    #add both entries to hbase
    write_hbase_event(id_hash, expiration, data)
    write_hbase_expiration_index(id_hash, expiration)

def move_event_to_redis(id, expiration):
    id_hash = generate_hash(id)

    #get event from hbase
    event = read_hbase_event(id)

    #TODO better exception handling
    if event is None:
        print "ERROR copying event " + id_hash + " to redis, data not found"
        return

    #delete both entries from hbase
    delete_hbase_event(id_hash)
    delete_hbase_expiration(id_hash, event.expiration)

    if expiration is not None:
        event.expiration = expiration

    #add to redis
    pipe = redis_server.pipeline()
    pipe.zadd(EXPIRATION_QUEUE, id_hash, event.expiration)
    pipe.hset(EVENT_QUEUE, id_hash, event.payload)
    pipe.execute()

class queue_consumer(threading.Thread):
    def __init__(self):
        super(queue_consumer, self).__init__()

    def run(self):
        while True:
            now = calendar.timegm(datetime.datetime.utcnow().utctimetuple())
            id_hash = redis_server.zrangebyscore(EXPIRATION_QUEUE, "-inf", now, None, None, True)
            
            #if there is no work to do, sleep for 1 second
            if (len(id_hash) == 0):
                time.sleep(1)
            else:
                for tuple in id_hash:
                    pipe = redis_server.pipeline()
                    pipe.hget(EVENT_QUEUE, tuple[0])
                    pipe.hdel(EVENT_QUEUE, tuple[0])
                    pipe.zrem(EXPIRATION_QUEUE, tuple[0])
                    redis_response = pipe.execute()
                    consumer_response = requests.put(FES_CONSUMER_URL, data=json.dumps(str(redis_response[0])), headers={'Content-Type': 'application/json'})

                    #TODO delete me
                    print "expiring event: " + str(redis_response[0]) 

if __name__ == '__main__':
    queueConsumer = queue_consumer()
    queueConsumer.start()
    app.run(debug=True)