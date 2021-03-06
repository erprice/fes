import marshalling_agent
import hbase_data
import redis_data
import fes_controller
from FesException import FesException
import test_utils

def test_scan_for_hbase_expirations():
    id_hash = test_utils.get_random_string()
    #now plus 15 minutes, minus 1 second
    expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) - 1
    rowkey_prefix = id_hash[:1] + "_"

    #add an event that is set to expire from hbase right now
    hbase_data.add(id_hash, expiration, test_utils.TEST_PAYLOAD)

    id_hashes = marshalling_agent._scan_for_hbase_expirations(rowkey_prefix)

    assert id_hashes[-1] == id_hash

def test_marshall_event_into_redis():
    id_hash = test_utils.get_random_string()
    expiration = test_utils.get_random_int()

    #add an event that is set to expire from hbase right now
    hbase_data.add(id_hash, expiration, test_utils.TEST_PAYLOAD)

    marshalling_agent._marshall_event_into_redis(id_hash)

    #ensure it's been deleted from hbase
    future_event = hbase_data.read_event(id_hash)
    assert future_event is None

    #check the payload end expiration in redis
    assert redis_data.get_event_payload(id_hash) == test_utils.TEST_PAYLOAD
    assert redis_data.get_expiration(id_hash) == expiration

def test_marshall_event_into_redis_event_not_found():
    id_hash = test_utils.get_random_string()
    expiration = test_utils.get_random_int()

    #just create an index entry, no data in the fes_event table
    hbase_data._write_expiration_index(id_hash, expiration)

    try:
        marshalling_agent._marshall_event_into_redis(id_hash)
        assert False
    except FesException as e:
        assert e.value == "Failed to retrieve event from hbase hash_id=" + id_hash
