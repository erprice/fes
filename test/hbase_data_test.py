import hbase_data
import base64

def test_generate_salted_rowkey():
    rowkey = "asdf"
    expiration = 123456
    salted_rowkey = "a_123456"

    assert hbase_data._generate_salted_row_key(rowkey, expiration) == salted_rowkey

def test_marshall_event_from_hbase_response():
    payload = '{"a list" : ["a list item"], "a string" : "string value"}'
    expiration = "12345"
    hash_id = "asdf"

    data = {
        'Row': [
            {
                'Cell': [
                    {
                        'column': base64.b64encode(hbase_data.COLUMN_FAMILY + ":" + hbase_data.PAYLOAD_COLUMN),
                        'timestamp': 1418015414582, 
                        '$': base64.b64encode(payload)
                    }, 
                    {
                        'column': base64.b64encode(hbase_data.COLUMN_FAMILY + ":" + hbase_data.EXPIRATION_COLUMN),
                        'timestamp': 1418015414582,
                        '$': base64.b64encode(expiration)
                    }
                ],
                'key': base64.b64encode(hash_id)
            }
        ]
    }

    future_event = hbase_data._marshall_event_from_hbase_response(data)

    assert future_event.payload == payload
    assert future_event.expiration == expiration
    assert future_event.id_ is None