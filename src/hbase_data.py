from ordereddict import OrderedDict
import requests
import json
import base64
from future_event import future_event

HBASE_BASE_URL = "http://localhost:8080"

EVENT_TABLE = 'fes_event'
EXPIRATION_TABLE = 'fes_expiration'

COLUMN_FAMILY = 'attrs'

PAYLOAD_COLUMN = "payload"
EXPIRATION_COLUMN = "expiration"

def write_hbase_event(id, expiration, payload):
    rowkey = id
    url = HBASE_BASE_URL + "/" + EVENT_TABLE + "/" + rowkey + "/" + COLUMN_FAMILY + ":" + PAYLOAD_COLUMN
    hbase_data = _generate_event_table_write_data(rowkey, str(expiration), payload)

    _write_to_hbase(url, hbase_data)

def write_hbase_expiration_index(id, expiration):
    rowkey = _generate_salted_row_key(id, expiration)
    url = HBASE_BASE_URL + "/" + EXPIRATION_TABLE + "/" + rowkey + "/" + COLUMN_FAMILY + ":" + EXPIRATION_COLUMN
    hbase_data = _generate_index_write_data(rowkey, id)

    _write_to_hbase(url, hbase_data)

def delete_hbase_event(id_hash):
    url = HBASE_BASE_URL + "/" + EVENT_TABLE + "/" + id_hash
    hbase_response = requests.delete(url)

def delete_hbase_expiration(id_hash, expiration):
    rowkey = _generate_salted_row_key(id_hash, expiration)
    url = HBASE_BASE_URL + "/" + EXPIRATION_TABLE + "/" + rowkey
    hbase_response = requests.delete(url)

def read_hbase_event(id_hash):
    url = HBASE_BASE_URL + "/" + EVENT_TABLE + "/" + id_hash

    headers = {"Accept" : "application/json"}

    hbase_response = requests.get(url, headers=headers)
    hbase_data = json.loads(hbase_response.text)
    futureEvent = _get_event_from_hbase_response(hbase_data)

    return futureEvent;

"""salt the start of the hbase rowkey to prevent hotspotting on writes"""
def _generate_salted_row_key(key, expiration):
    return key[:1] + "_" + str(expiration)

def _write_to_hbase(url, data):
    headers = {"Content-Type" : "application/json"}
    hbase_response = requests.put(url, data=data, headers=headers)

def _generate_event_table_write_data(id, expiration, payload):
    column_value_dict = {
        COLUMN_FAMILY + ":" + PAYLOAD_COLUMN : payload,
        COLUMN_FAMILY + ":" + EXPIRATION_COLUMN : str(expiration)
    }

    return _generate_hbase_write_data(id, column_value_dict)

def _generate_index_write_data(id, expiration):
    return _generate_hbase_write_data(id, { COLUMN_FAMILY + ":" + EXPIRATION_COLUMN : str(expiration) } )

def _generate_hbase_write_data(rowkey, column_value_dict):
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

def _get_event_from_hbase_response(data):
    for row in data['Row']:
        payload = ""
        expiration = ""
        
        for cell in row['Cell']:
            column = base64.b64decode(cell['column'])
            value = cell['$']

            if value is None:
                continue

            if column == COLUMN_FAMILY + ":" + PAYLOAD_COLUMN:
                payload = base64.b64decode(value)
            elif column == COLUMN_FAMILY + ":" + EXPIRATION_COLUMN:
                expiration = base64.b64decode(str(value))

    return future_event(None, payload, expiration)

def _get_id_hash_from_hbase_response(data):
    for row in data['Row']:
        expiration = ""
        
        for cell in row['Cell']:
            column = base64.b64decode(cell['column'])
            value = cell['$']

            if value is None:
                continue

            if column == COLUMN_FAMILY + ":" + EXPIRATION_COLUMN:
                expiration = base64.b64decode(str(value))

    return expiration
