from flask import Flask, jsonify, make_response, abort, request
from ordereddict import OrderedDict
import redis
import datetime
import calendar
import hashlib
import threading
import time
import requests
import json
import base64

app = Flask(__name__)
redis_server = redis.Redis("localhost")

EXPIRATIONS_QUEUE = "fes:EXPIRATIONS_QUEUE"
EVENTS_QUEUE = "fes:EVENTS_QUEUE"

HBASE_BASE_URL = "http://localhost:8080"

EVENTS_TABLE = 'fes_event'
EXPIRATIONS_TABLE = 'fes_expiration'

COLUMN_FAMILY = 'attrs'

PAYLOAD_COLUMN = "payload"
EXPIRATION_COLUMN = "expiration"

@app.route('/add/<string:id>/<int:expiration>', methods=['POST'])
def add(id, expiration):
    now = datetime.datetime.utcnow()

    if request.json == None or expiration <= calendar.timegm(now.utctimetuple()):
        abort(400)
    event = FutureEvent(id, request.json, expiration)

    hash = generate_hash(id)

    fifteen_minutes_from_now = now + datetime.timedelta(minutes=15)

    if expiration <= calendar.timegm(fifteen_minutes_from_now.utctimetuple()):
        pipe = redis_server.pipeline()
        pipe.zadd(EXPIRATIONS_QUEUE, hash, expiration)
        pipe.hset(EVENTS_QUEUE, hash, event.payload)
        pipe.execute()
    else:
        write_event(hash, expiration, json.dumps(event.payload))
        write_expiration_index(hash, expiration)

    return jsonify(event.__dict__), 201

@app.route('/update/expiration/<string:id>/<int:expiration>', methods=['PUT'])
def update_expiration(id, expiration):
    now = calendar.timegm(datetime.datetime.utcnow().utctimetuple())
    if expiration <= now:
            abort(400)

    hash = generate_hash(id)

    pipe = redis_server.pipeline()
    pipe.zrem(EXPIRATIONS_QUEUE, hash)
    pipe.zadd(EXPIRATIONS_QUEUE, hash, expiration)
    pipe.execute()
    
    return jsonify({}), 200

@app.route('/update/event/<string:id>', methods=['PUT'])
def update_event(id):
    if request.json == None:
            abort(400)

    redis_server.hset(EVENTS_QUEUE, generate_hash(id), request.json)
    return jsonify({}), 200

@app.route('/delete/<string:id>', methods=['DELETE'])
def delete(id):

    hash = generate_hash(id)

    pipe = redis_server.pipeline()
    pipe.zrem(EXPIRATIONS_QUEUE, hash)
    pipe.hdel(EVENTS_QUEUE, hash)
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

def generate_event_table_write_data(id, expiration, payload):
    column_value_dict = {
        COLUMN_FAMILY + ":" + PAYLOAD_COLUMN : payload,
        COLUMN_FAMILY + ":" + EXPIRATION_COLUMN : str(expiration)
    }

    print column_value_dict
    return generate_hbase_write_data(id, column_value_dict)

def generate_index_write_data(id, expiration):
    return generate_hbase_write_data(id, { COLUMN_FAMILY + ":" + EXPIRATION_COLUMN : str(expiration) } )

def generate_hbase_write_data(rowkey, column_value_dict):
    row = OrderedDict([
        ("key", base64.b64encode(rowkey))
    ])

    cells = []
    cell = {"Cell" : cells}
    for key in column_value_dict:
        cell_entries = {
            "column" : base64.b64encode(key),
            "$" : base64.b64encode(column_value_dict[key])
        }
        cells.append(cell_entries)

    row.update( { "Cell" : cells} )
    data = { "Row" : [cell] }

    return json.dumps(data)

#salt the start of the hbase rowkey to prevent hotspotting on writes
def generate_salted_row_key(key, expiration):
    return key[:1] + "_" + str(expiration)

def write_event(id, expiration, payload):
    rowkey = id
    url = HBASE_BASE_URL + "/" + EVENTS_TABLE + "/" + rowkey + "/" + COLUMN_FAMILY + ":" + PAYLOAD_COLUMN
    hbase_data = generate_event_table_write_data(rowkey, str(expiration), payload)

    write_to_hbase(url, hbase_data)

def write_expiration_index(id, expiration):
    rowkey = generate_salted_row_key(id, expiration)
    url = HBASE_BASE_URL + "/" + EXPIRATIONS_TABLE + "/" + rowkey + "/" + COLUMN_FAMILY + ":" + EXPIRATION_COLUMN
    hbase_data = generate_index_write_data(rowkey, id)

    write_to_hbase(url, hbase_data)

def write_to_hbase(url, data):
    headers = {"Content-Type" : "application/json"}

    hbase_response = requests.put(url, data=data, headers=headers)
    
    #TODO delete me
    print hbase_response

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
            hash = redis_server.zrangebyscore(EXPIRATIONS_QUEUE, "-inf", now, None, None, True)
            
            #if there is no work to do, sleep for 1 second
            if (len(hash) == 0):
                time.sleep(1)
            else:
                pipe = redis_server.pipeline()
                for tuple in hash:

                    pipe.hget(EVENTS_QUEUE, tuple[0])
                    pipe.hdel(EVENTS_QUEUE, tuple[0])
                    pipe.zrem(EXPIRATIONS_QUEUE, tuple[0])
                    redis_response = pipe.execute()
                    hbase_response = requests.put(FES_CONSUMER_URL, data=json.dumps(str(redis_response[0])), headers={'Content-Type': 'application/json'})

                    #TODO delete me
                    print "expiring event: " + str(redis_response[0]) 

if __name__ == '__main__':
    queueConsumer = QueueConsumer()
    queueConsumer.start()
    app.run(debug=True)