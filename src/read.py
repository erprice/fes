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

HBASE_BASE_URL = "http://localhost:8080"

EVENTS_TABLE = 'fes_event'
EXPIRATIONS_TABLE = 'fes_expiration'

COLUMN_FAMILY = 'attrs'

PAYLOAD_COLUMN = "payload"
EXPIRATION_COLUMN = "expiration"

def generate_hash(id):
    return hashlib.sha224(id).hexdigest()

#salt the start of the hbase rowkey to prevent hotspotting on writes
def generate_salted_row_key(key, expiration):
    return key[:1] + "_" + str(expiration)



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








    

def read_hbase_event(key):
    rowkey = generate_hash(key)
    url = HBASE_BASE_URL + "/" + EVENTS_TABLE + "/" + rowkey

    headers = {"Accept" : "application/json"}

    hbase_response = requests.get(url, headers=headers)
    hbase_data = json.loads(hbase_response.text)
    futureEvent = get_event_from_hbase_response(key, hbase_data)

    return futureEvent;

def read_hbase_expiration_index(key, expiration):
    rowkey = generate_salted_row_key(generate_hash(key), expiration)
    url = HBASE_BASE_URL + "/" + EXPIRATIONS_TABLE + "/" + rowkey

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


print read_hbase_event("asdf")
print read_hbase_expiration_index("asdf", 1417017324)