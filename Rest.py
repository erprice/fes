from flask import Flask, jsonify, make_response, abort, request
import redis
import datetime
import calendar
import hashlib
import threading
import time
import requests

app = Flask(__name__)
redis_server = redis.Redis("localhost")

EXPIRATIONS = "fes:expirations"
EVENTS = "fes:events"

FES_CONSUMER_URL = "http://localhost:5001/expiration"

@app.route('/add/<string:id>/<int:expiration>', methods=['POST'])
def add(id, expiration):
    now = calendar.timegm(datetime.datetime.utcnow().utctimetuple())

    if request.json == None or expiration <= now:
        abort(400)
    event = FutureEvent(id, request.json, expiration)

    hash = generate_hash(id)

    pipe = redis_server.pipeline()
    pipe.zadd(EXPIRATIONS, hash, expiration)
    pipe.hset(EVENTS, hash, event.payload)
    pipe.execute()

    return jsonify(event.__dict__), 201

@app.route('/update/expiration/<string:id>/<int:expiration>', methods=['PUT'])
def update_expiration(id, expiration):
    now = calendar.timegm(datetime.datetime.utcnow().utctimetuple())
    if expiration <= now:
            abort(400)

    hash = generate_hash(id)

    pipe = redis_server.pipeline()
    pipe.zrem(EXPIRATIONS, hash)
    pipe.zadd(EXPIRATIONS, hash, expiration)
    pipe.execute()
    
    return jsonify({}), 200

@app.route('/update/event/<string:id>', methods=['PUT'])
def update_event(id):
    if request.json == None:
            abort(400)

    redis_server.hset(EVENTS, generate_hash(id), request.json)
    return jsonify({}), 200

@app.route('/delete/<string:id>', methods=['DELETE'])
def delete(id):

    hash = generate_hash(id)

    pipe = redis_server.pipeline()
    pipe.zrem(EXPIRATIONS, hash)
    pipe.hdel(EVENTS, hash)
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

class FutureEvent:
    def __init__(self, id, payload, expiration):
        self.id = id
        self.payload = payload
        self.expiration = expiration

    def __repr__(self):
        return '%s: {"id" : %s, "expiration" : %s, "payload" : %s}' % (
            self.__class__.__name__, self.id, self.expiration, self.payload)

class QueueConsumer(threading.Thread):
    def __init__(self):
        super(QueueConsumer, self).__init__()

    def run(self):
        while True:
            now = calendar.timegm(datetime.datetime.utcnow().utctimetuple())
            hash = redis_server.zrangebyscore(EXPIRATIONS, "-inf", now, None, None, True)
            
            #if there is no work to do, sleep for 1 second
            if (len(hash) == 0):
                time.sleep(1)
            else:
                pipe = redis_server.pipeline()
                for tuple in hash:
                    pipe.hget(EVENTS, tuple[0])
                    pipe.hdel(EVENTS, tuple[0])
                    pipe.zrem(EXPIRATIONS, tuple[0])
                    response = pipe.execute()
                    requests.put(FES_CONSUMER_URL, data=str(response[0]), headers={'content-type': 'application/json'})
                    print "expiring event: " + str(response[0])

if __name__ == '__main__':
    queueConsumer = QueueConsumer()
    queueConsumer.start()
    app.run(debug=True)