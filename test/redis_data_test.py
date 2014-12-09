import redis_data
import uuid
from random import randint

TEST_PAYLOAD = '{"a list" : ["a list item"], "a string" : "string value"}'

def get_random_string():
    return str(uuid.uuid1())

"""get a random 10 digit int"""
def get_random_int():
    return randint(1000000000, 9999999999)

def test_add_and_get_payload():
    id_hash = get_random_string()
    expiration = get_random_int()

    redis_data.add(id_hash, expiration, TEST_PAYLOAD)
    result = redis_data.get_event_payload(id_hash)

    assert result == TEST_PAYLOAD

def test_add_and_get_expiration():
    id_hash = get_random_string()
    expiration = get_random_int()

    redis_data.add(id_hash, expiration, TEST_PAYLOAD)
    result = redis_data.get_expiration(id_hash)

    assert result == expiration

def test_update_expiration():
    id_hash = get_random_string()
    expiration = get_random_int()

    redis_data.add(id_hash, get_random_int(), TEST_PAYLOAD)
    redis_data.update_expiration(id_hash, expiration)
    result = redis_data.get_expiration(id_hash)

    assert result == expiration

def test_update_event():
    id_hash = get_random_string()
    expiration = get_random_int()

    redis_data.add(id_hash, get_random_int(), TEST_PAYLOAD)
    redis_data.update_expiration(id_hash, expiration)
    result = redis_data.get_expiration(id_hash)

    assert result == expiration

def test_delete():
    id_hash = get_random_string()
    expiration = get_random_int()

    redis_data.add(id_hash, expiration, TEST_PAYLOAD)
    redis_data.delete(id_hash)
    result = redis_data.get_expiration(id_hash)

    assert result is None

def test_get_and_delete():
    id_hash = get_random_string()
    expiration = get_random_int()

    redis_data.add(id_hash, expiration, TEST_PAYLOAD)
    future_event = redis_data.get_and_delete(id_hash)

    assert future_event.id_ is None
    assert future_event.expiration == expiration
    assert future_event.payload == TEST_PAYLOAD

    assert redis_data.get_event_payload(id_hash) is None
    assert redis_data.get_expiration(id_hash) is None

def test_get_expiration_range_upper_and_lower_bounded():
    start_time = get_random_int()
    expected_results = []

    for i in range(0, 10):
        id_hash = get_random_string()
        expiration = start_time + i

        expected_results.append((id_hash , float(expiration)))
        redis_data.add(id_hash, expiration, TEST_PAYLOAD)

    #scan the inner 6 entries, leave out the outer two on each side
    results = redis_data.get_expiration_range(start_time + 2, start_time + 7)

    assert results == expected_results[2:8]

def test_get_expiration_range_upper_bounded():
    start_time = get_random_int()
    expected_results = []

    for i in range(0, 10):
        id_hash = get_random_string()
        expiration = start_time + i

        expected_results.append((id_hash , float(expiration)))
        redis_data.add(id_hash, expiration, TEST_PAYLOAD)

    #scan every entry before the last two
    results = redis_data.get_expiration_range("-inf", start_time + 7)

    assert results[-8:] == expected_results[:8]

def test_get_expiration_range_lower_bounded():
    start_time = get_random_int()
    expected_results = []

    for i in range(0, 10):
        id_hash = get_random_string()
        expiration = start_time + i

        expected_results.append((id_hash , float(expiration)))
        redis_data.add(id_hash, expiration, TEST_PAYLOAD)

    #scan every entry after the first two
    results = redis_data.get_expiration_range(start_time + 2, "+inf")

    assert results[:8] == expected_results[2:]










