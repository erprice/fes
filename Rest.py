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

EXPIRATION_QUEUE = "fes:expiration_queue"
EVENT_QUEUE = "fes:event_queue"

FES_CONSUMER_URL = "http://localhost:5001/expiration"
HBASE_BASE_URL = "http://localhost:8080"

EVENT_TABLE = 'fes_event'
EXPIRATION_TABLE = 'fes_expiration'

COLUMN_FAMILY = 'attrs'

PAYLOAD_COLUMN = "payload"
EXPIRATION_COLUMN = "expiration"

@app.route('/add/<string:id>/<int:expiration>', methods=['POST'])
def add(id, expiration):
    now = datetime.datetime.utcnow()

    if request.json == None or expiration <= calendar.timegm(now.utctimetuple()):
        abort(400)
    event = FutureEvent(id, request.json, expiration)

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
    if redis_score != None:
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
    if event != None:
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
    if request.json == None:
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

def generate_event_table_write_data(id, expiration, payload):
    column_value_dict = {
        COLUMN_FAMILY + ":" + PAYLOAD_COLUMN : payload,
        COLUMN_FAMILY + ":" + EXPIRATION_COLUMN : str(expiration)
    }

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

def write_hbase_event(id, expiration, payload):
    rowkey = id
    url = HBASE_BASE_URL + "/" + EVENT_TABLE + "/" + rowkey + "/" + COLUMN_FAMILY + ":" + PAYLOAD_COLUMN
    hbase_data = generate_event_table_write_data(rowkey, str(expiration), payload)

    write_to_hbase(url, hbase_data)

def write_hbase_expiration_index(id, expiration):
    rowkey = generate_salted_row_key(id, expiration)
    url = HBASE_BASE_URL + "/" + EXPIRATION_TABLE + "/" + rowkey + "/" + COLUMN_FAMILY + ":" + EXPIRATION_COLUMN
    hbase_data = generate_index_write_data(rowkey, id)

    write_to_hbase(url, hbase_data)

def read_hbase_expiration_index(id_hash):
    rowkey = generate_salted_row_key(id_hash, expiration)
    url = HBASE_BASE_URL + "/" + EXPIRATION_TABLE + "/" + rowkey + "/" + COLUMN_FAMILY + ":" + EXPIRATION_COLUMN
    headers = {"Accept" : "application/json"}

    hbase_response = requests.put(url, data=data, headers=headers)

def write_to_hbase(url, data):
    headers = {"Content-Type" : "application/json"}
    hbase_response = requests.put(url, data=data, headers=headers)

def delete_hbase_event(id_hash):
    url = HBASE_BASE_URL + "/" + EVENT_TABLE + "/" + id_hash
    hbase_response = requests.delete(url)

def delete_hbase_expiration(id_hash, expiration):
    rowkey = generate_salted_row_key(id_hash, expiration)
    url = HBASE_BASE_URL + "/" + EXPIRATION_TABLE + "/" + rowkey
    hbase_response = requests.delete(url)

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
    if data == None:
        print "ERROR copying event " + id_hash + " to hbase, data not found"
        return

    if expiration == None:
        expiration = int(redis_response[1])

    #add both entries to hbase
    write_hbase_event(id_hash, expiration, data)
    write_hbase_expiration_index(id_hash, expiration)

def move_event_to_redis(id, expiration):
    id_hash = generate_hash(id)

    #get event from hbase
    event = read_hbase_event(id)

    #TODO better exception handling
    if event == None:
        print "ERROR copying event " + id_hash + " to redis, data not found"
        return

    #delete both entries from hbase
    delete_hbase_event(id_hash)
    delete_hbase_expiration(id_hash, event.expiration)

    if expiration != None:
        event.expiration = expiration

    #add to redis
    pipe = redis_server.pipeline()
    pipe.zadd(EXPIRATION_QUEUE, id_hash, event.expiration)
    pipe.hset(EVENT_QUEUE, id_hash, event.payload)
    pipe.execute()

def read_hbase_event(key):
    rowkey = generate_hash(key)
    url = HBASE_BASE_URL + "/" + EVENT_TABLE + "/" + rowkey

    headers = {"Accept" : "application/json"}

    hbase_response = requests.get(url, headers=headers)
    hbase_data = json.loads(hbase_response.text)
    futureEvent = get_event_from_hbase_response(key, hbase_data)

    return futureEvent;

def read_hbase_expiration_index(key, expiration):
    rowkey = generate_salted_row_key(generate_hash(key), expiration)
    url = HBASE_BASE_URL + "/" + EXPIRATION_TABLE + "/" + rowkey

    headers = {"Accept" : "application/json"}

    hbase_response = requests.get(url, headers=headers)
    hbase_data = json.loads(hbase_response.text)

    return get_id_hash_from_hbase_response(hbase_data)

def get_event_from_hbase_response(key, data):
    for row in data['Row']:
        payload = ""
        expiration = ""
        
        for cell in row['Cell']:
            column = base64.b64decode(cell['column'])
            value = cell['$']

            if value == None:
                continue

            if column == COLUMN_FAMILY + ":" + PAYLOAD_COLUMN:
                payload = base64.b64decode(value)
            elif column == COLUMN_FAMILY + ":" + EXPIRATION_COLUMN:
                expiration = base64.b64decode(str(value))

    return FutureEvent(key, payload, expiration)

def get_id_hash_from_hbase_response(data):
    for row in data['Row']:
        expiration = ""
        
        for cell in row['Cell']:
            column = base64.b64decode(cell['column'])
            value = cell['$']

            if value == None:
                continue

            if column == COLUMN_FAMILY + ":" + EXPIRATION_COLUMN:
                expiration = base64.b64decode(str(value))

    return expiration

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
            id_hash = redis_server.zrangebyscore(EXPIRATION_QUEUE, "-inf", now, None, None, True)
            
            #if there is no work to do, sleep for 1 second
            if (len(id_hash) == 0):
                time.sleep(1)
            else:
                pipe = redis_server.pipeline()
                for tuple in id_hash:

                    pipe.hget(EVENT_QUEUE, tuple[0])
                    pipe.hdel(EVENT_QUEUE, tuple[0])
                    pipe.zrem(EXPIRATION_QUEUE, tuple[0])
                    redis_response = pipe.execute()
                    consumer_response = requests.put(FES_CONSUMER_URL, data=json.dumps(str(redis_response[0])), headers={'Content-Type': 'application/json'})

                    #TODO delete me
                    print "expiring event: " + str(redis_response[0]) 

if __name__ == '__main__':
    queueConsumer = QueueConsumer()
    queueConsumer.start()
    app.run(debug=True)