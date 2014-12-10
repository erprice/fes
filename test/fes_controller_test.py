import fes_controller
import redis_data
import hbase_data
import test_utils

UPDATED_TEST_PAYLOAD = '{"a different list" : "some other stuff", "hark, a test datum" : ["this time with a", "two item list"]}'

def test_generate_hash():
    id_hashes = {}

    for i in range(100):
        id_ = test_utils.get_random_string()
        id_hash = fes_controller.generate_hash(id_)

        id_hashes[id_hash] = id_

    assert len(id_hashes) == 100

"""===============================================test add====================================================="""

def test_add_redis_only():
    id_ = test_utils.get_random_string()
    id_hash = fes_controller.generate_hash(id_)
    expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) - 10

    fes_controller.add(id_, expiration, test_utils.TEST_PAYLOAD)

    assert redis_data.get_expiration(id_hash) == expiration
    assert redis_data.get_event_payload(id_hash) == test_utils.TEST_PAYLOAD
    assert hbase_data.read_event(id_hash) is None

def test_add_hbase_only():
    id_ = test_utils.get_random_string()
    id_hash = fes_controller.generate_hash(id_)
    expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) + 10

    fes_controller.add(id_, expiration, test_utils.TEST_PAYLOAD)

    assert redis_data.get_expiration(id_hash) is None
    assert redis_data.get_event_payload(id_hash) is None
    future_event = hbase_data.read_event(id_hash)
    assert future_event is not None
    assert future_event.id_ is None
    assert future_event.expiration == str(expiration)
    assert future_event.payload == test_utils.TEST_PAYLOAD

def test_add_to_hbase_already_in_hbase():
    id_ = test_utils.get_random_string()
    id_hash = fes_controller.generate_hash(id_)
    original_expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) + 10
    updated_expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) + 15

    fes_controller.add(id_, original_expiration, test_utils.TEST_PAYLOAD)
    fes_controller.add(id_, updated_expiration, UPDATED_TEST_PAYLOAD)

    assert redis_data.get_expiration(id_hash) is None
    assert redis_data.get_event_payload(id_hash) is None
    
    future_event = hbase_data.read_event(id_hash)
    assert future_event is not None
    assert future_event.id_ is None
    assert future_event.expiration == str(updated_expiration)
    assert future_event.payload == UPDATED_TEST_PAYLOAD

def test_add_to_redis_already_in_redis():
    id_ = test_utils.get_random_string()
    id_hash = fes_controller.generate_hash(id_)
    original_expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) - 10
    updated_expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) - 15

    fes_controller.add(id_, original_expiration, test_utils.TEST_PAYLOAD)
    fes_controller.add(id_, updated_expiration, UPDATED_TEST_PAYLOAD)
    
    assert redis_data.get_expiration(id_hash) == updated_expiration
    assert redis_data.get_event_payload(id_hash) == UPDATED_TEST_PAYLOAD
    assert hbase_data.read_event(id_hash) is None

def test_add_to_hbase_already_in_redis():
    id_ = test_utils.get_random_string()
    id_hash = fes_controller.generate_hash(id_)
    original_expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) - 10
    updated_expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) + 10

    fes_controller.add(id_, original_expiration, test_utils.TEST_PAYLOAD)
    fes_controller.add(id_, updated_expiration, UPDATED_TEST_PAYLOAD)
    
    assert redis_data.get_expiration(id_hash) is None
    assert redis_data.get_event_payload(id_hash) is None
    future_event = hbase_data.read_event(id_hash)
    assert future_event is not None
    assert future_event.id_ is None
    assert future_event.expiration == str(updated_expiration)
    assert future_event.payload == UPDATED_TEST_PAYLOAD

def test_add_to_redis_already_in_hbase():
    id_ = test_utils.get_random_string()
    id_hash = fes_controller.generate_hash(id_)
    original_expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) + 10
    updated_expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) - 10

    fes_controller.add(id_, original_expiration, test_utils.TEST_PAYLOAD)
    fes_controller.add(id_, updated_expiration, UPDATED_TEST_PAYLOAD)
    
    assert redis_data.get_expiration(id_hash) == updated_expiration
    assert redis_data.get_event_payload(id_hash) == UPDATED_TEST_PAYLOAD
    assert hbase_data.read_event(id_hash) is None

"""==================================================test update expiration==============================================="""

def test_update_expiration_to_redis_already_in_redis():
    id_ = test_utils.get_random_string()
    id_hash = fes_controller.generate_hash(id_)
    original_expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) - 10
    updated_expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) - 15

    fes_controller.add(id_, original_expiration, test_utils.TEST_PAYLOAD)
    fes_controller.update_expiration(id_, updated_expiration)
    
    assert redis_data.get_expiration(id_hash) == updated_expiration
    assert redis_data.get_event_payload(id_hash) == test_utils.TEST_PAYLOAD
    assert hbase_data.read_event(id_hash) is None

def test_update_expiration_to_hbase_already_in_hbase():
    id_ = test_utils.get_random_string()
    id_hash = fes_controller.generate_hash(id_)
    original_expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) + 10
    updated_expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) + 15

    fes_controller.add(id_, original_expiration, test_utils.TEST_PAYLOAD)
    fes_controller.update_expiration(id_, updated_expiration)

    assert redis_data.get_expiration(id_hash) is None
    assert redis_data.get_event_payload(id_hash) is None
    
    future_event = hbase_data.read_event(id_hash)
    assert future_event is not None
    assert future_event.id_ is None
    assert future_event.expiration == str(updated_expiration)
    assert future_event.payload == test_utils.TEST_PAYLOAD

def test_update_expiration_to_hbase_already_in_redis():
    id_ = test_utils.get_random_string()
    id_hash = fes_controller.generate_hash(id_)
    original_expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) - 10
    updated_expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) + 10

    fes_controller.add(id_, original_expiration, test_utils.TEST_PAYLOAD)
    fes_controller.update_expiration(id_, updated_expiration)
    
    assert redis_data.get_expiration(id_hash) is None
    assert redis_data.get_event_payload(id_hash) is None
    future_event = hbase_data.read_event(id_hash)
    assert future_event is not None
    assert future_event.id_ is None
    assert future_event.expiration == str(updated_expiration)
    assert future_event.payload == test_utils.TEST_PAYLOAD

def test_update_expiration_to_redis_already_in_hbase():
    id_ = test_utils.get_random_string()
    id_hash = fes_controller.generate_hash(id_)
    original_expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) + 10
    updated_expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) - 10

    fes_controller.add(id_, original_expiration, test_utils.TEST_PAYLOAD)
    fes_controller.update_expiration(id_, updated_expiration)
    
    assert redis_data.get_expiration(id_hash) == updated_expiration
    assert redis_data.get_event_payload(id_hash) == test_utils.TEST_PAYLOAD
    assert hbase_data.read_event(id_hash) is None

"""==================================================test update event==============================================="""

def test_update_event_payload_in_redis():
    id_ = test_utils.get_random_string()
    id_hash = fes_controller.generate_hash(id_)
    expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) - 10

    fes_controller.add(id_, expiration, test_utils.TEST_PAYLOAD)
    fes_controller.update_event_payload(id_, UPDATED_TEST_PAYLOAD)
    
    assert int(redis_data.get_expiration(id_hash)) == expiration
    assert redis_data.get_event_payload(id_hash) == UPDATED_TEST_PAYLOAD
    assert hbase_data.read_event(id_hash) is None

def test_update_event_payload_in_hbase():
    id_ = test_utils.get_random_string()
    id_hash = fes_controller.generate_hash(id_)
    expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) + 10

    fes_controller.add(id_, expiration, test_utils.TEST_PAYLOAD)
    fes_controller.update_event_payload(id_, UPDATED_TEST_PAYLOAD)

    assert redis_data.get_expiration(id_hash) is None
    assert redis_data.get_event_payload(id_hash) is None
    
    future_event = hbase_data.read_event(id_hash)
    assert future_event is not None
    assert future_event.id_ is None
    assert future_event.expiration == str(expiration)
    assert future_event.payload == UPDATED_TEST_PAYLOAD

"""==================================================test delete==============================================="""
def test_delete_from_redis():
    id_ = test_utils.get_random_string()
    id_hash = fes_controller.generate_hash(id_)
    expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) - 10

    fes_controller.add(id_, expiration, test_utils.TEST_PAYLOAD)
    fes_controller.delete(id_)

    assert redis_data.get_expiration(id_hash) is None
    assert redis_data.get_event_payload(id_hash) is None
    assert hbase_data.read_event(id_hash) is None

def test_delete_from_hbase():
    id_ = test_utils.get_random_string()
    id_hash = fes_controller.generate_hash(id_)
    expiration = test_utils.get_current_timestamp() + (fes_controller.STORAGE_CUTOFF_MINUTES * 60) + 10

    fes_controller.add(id_, expiration, test_utils.TEST_PAYLOAD)
    fes_controller.delete(id_)

    assert redis_data.get_expiration(id_hash) is None
    assert redis_data.get_event_payload(id_hash) is None
    assert hbase_data.read_event(id_hash) is None