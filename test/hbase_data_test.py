import hbase_data
import base64
import uuid
from random import randint

TEST_PAYLOAD = '{"a list" : ["a list item"], "a string" : "string value"}'

def get_random_string():
    return str(uuid.uuid1())

"""get a random 10 digit int"""
def get_random_int():
    return randint(1000000000, 9999999999)

def test_generate_salted_rowkey():
    rowkey = "asdf"
    expiration = 123456
    salted_rowkey = "a_123456"

    assert hbase_data._generate_salted_row_key(rowkey, expiration) == salted_rowkey

def test_marshall_event_from_hbase_response():
    expiration = str(get_random_int())
    id_hash = get_random_string()

    data = {
        'Row': [
            {
                'Cell': [
                    {
                        'column': base64.b64encode(hbase_data.COLUMN_FAMILY + ":" + hbase_data.PAYLOAD_COLUMN),
                        'timestamp': 1418015414582, 
                        '$': base64.b64encode(TEST_PAYLOAD)
                    }, 
                    {
                        'column': base64.b64encode(hbase_data.COLUMN_FAMILY + ":" + hbase_data.EXPIRATION_COLUMN),
                        'timestamp': 1418015414582,
                        '$': base64.b64encode(expiration)
                    }
                ],
                'key': base64.b64encode(id_hash)
            }
        ]
    }

    future_event = hbase_data._marshall_event_from_hbase_response(data)

    assert future_event.payload == TEST_PAYLOAD
    assert future_event.expiration == expiration
    assert future_event.id_ is None

def test_get_id_hashes_from_hbase_response():
    id_hashes = [get_random_string(), get_random_string()]
    salted_rowkeys = [
        hbase_data._generate_salted_row_key(id_hashes[0], get_random_int()), 
        hbase_data._generate_salted_row_key(id_hashes[1], get_random_int())
    ]

    data = {
        'Row': [
            {
                'Cell': [
                    {
                        'column': base64.b64encode(hbase_data.COLUMN_FAMILY + ":" + id_hashes[0]),
                        'timestamp': 1418015414582, 
                        '$': base64.b64encode("")
                    }
                ],
                'key': base64.b64encode(salted_rowkeys[0])
            },
            {
                'Cell': [
                    {
                        'column': base64.b64encode(hbase_data.COLUMN_FAMILY + ":" + id_hashes[1]),
                        'timestamp': 1418015414582, 
                        '$': base64.b64encode("")
                    }
                ],
                'key': base64.b64encode(salted_rowkeys[1])
            }
        ]
    }

    result = hbase_data._get_id_hashes_from_hbase_response(data)

    assert result == id_hashes

def test_add_and_read_event():
    id_hash = get_random_string()
    expiration = str(get_random_int())

    hbase_data.add(id_hash, expiration, TEST_PAYLOAD)
    future_event = hbase_data.read_event(id_hash)

    assert future_event is not None
    assert future_event.id_ is None
    assert future_event.expiration == expiration
    assert future_event.payload == TEST_PAYLOAD

def test_add_and_read_expiration():
    id_hash = get_random_string()
    expiration = get_random_int()

    hbase_data.add(id_hash, expiration, TEST_PAYLOAD)
    start_row = hbase_data._generate_salted_row_key(id_hash, expiration)
    end_row = hbase_data._generate_salted_row_key(id_hash, expiration + 1)
    id_hashes = hbase_data.scan_expiration_index(start_row, end_row)
    assert id_hashes == [id_hash]

def test_add_and_delete_all():
    id_hash = get_random_string()
    expiration = str(get_random_int())

    hbase_data.add(id_hash, expiration, TEST_PAYLOAD)
    hbase_data.delete_all(id_hash, expiration)
    future_event = hbase_data.read_event(id_hash)

    assert future_event is None

def test_add_and_delete_from_expiration_index():
    id_hash = get_random_string()
    expiration = get_random_int()

    hbase_data.add(id_hash, expiration, TEST_PAYLOAD)
    hbase_data.delete_from_expiration_index(id_hash, expiration)
    
    #expiration is still stored on the event
    future_event = hbase_data.read_event(id_hash)
    assert future_event is not None
    assert future_event.id_ is None
    assert future_event.expiration == str(expiration)
    assert future_event.payload == TEST_PAYLOAD

    #but we won't be able to find it if we scan for it
    start_row = hbase_data._generate_salted_row_key(id_hash, expiration)
    end_row = hbase_data._generate_salted_row_key(id_hash, expiration + 1)
    id_hashes = hbase_data.scan_expiration_index(start_row, end_row)
    assert id_hashes is None