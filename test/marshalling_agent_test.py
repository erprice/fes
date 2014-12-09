from marshalling_agent import marshalling_agent
import hbase_data
import redis_data
import fes_controller
import calendar
import datetime
from FesException import FesException
import test_utils

def test_scan_for_hbase_expirations():
    id_hash = test_utils.get_random_string()
    #now plus 15 minutes, minus 1 second
    expiration = calendar.timegm(datetime.datetime.utcnow().utctimetuple()) + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) - 1
    rowkey_prefix = id_hash[:1] + "_"

    my_marshalling_agent = marshalling_agent(rowkey_prefix)

    #add an event that is set to expire from hbase right now
    hbase_data.add(id_hash, expiration, test_utils.TEST_PAYLOAD)

    id_hashes = my_marshalling_agent._scan_for_hbase_expirations()

    assert id_hashes[-1] == id_hash

def test_marshall_event_into_redis():
    id_hash = test_utils.get_random_string()
    expiration = test_utils.get_random_int()

    my_marshalling_agent = marshalling_agent("asdf")

    #add an event that is set to expire from hbase right now
    hbase_data.add(id_hash, expiration, test_utils.TEST_PAYLOAD)

    my_marshalling_agent._marshall_event_into_redis(id_hash)

    #ensure it's been deleted from hbase
    future_event = hbase_data.read_event(id_hash)
    assert future_event is None

    #check the payload end expiration in redis
    assert redis_data.get_event_payload(id_hash) == test_utils.TEST_PAYLOAD
    assert redis_data.get_expiration(id_hash) == expiration

def test_marshall_event_into_redis_event_not_found():
    id_hash = test_utils.get_random_string()
    expiration = test_utils.get_random_int()

    my_marshalling_agent = marshalling_agent("asdf")

    #just create an index entry, no data in the fes_event table
    hbase_data._write_expiration_index(id_hash, expiration)

    try:
        my_marshalling_agent._marshall_event_into_redis(id_hash)
        assert false
    except FesException as e:
        assert e.value == "Failed to retrieve event from hbase hash_id=" + id_hash