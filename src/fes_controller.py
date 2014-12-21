from future_event import future_event
import hbase_data
import redis_data
import hashlib
import datetime
import calendar
from FesException import FesException

STORAGE_CUTOFF_MINUTES = 15

def generate_hash(id_):
    return hashlib.sha224(id_).hexdigest()

def add(id_, expiration, payload):
    now = datetime.datetime.utcnow()
    if expiration <= calendar.timegm(now.utctimetuple()):
        raise FesException("Expiration must be in the future.")

    id_hash = generate_hash(id_)

    storage_cutoff_time = now + datetime.timedelta(minutes=STORAGE_CUTOFF_MINUTES)

    # check for an existing hbase record
    event = hbase_data.read_event(id_hash)

    #if the new expiration is within 15 minutes
    if expiration <= calendar.timegm(storage_cutoff_time.utctimetuple()):
        #delete hbase records
        if event is not None:
            hbase_data.delete_all(id_hash, event.expiration)

        #add to redis
        redis_data.add(id_hash, expiration, payload)
    else:
        #delete hbase index
        if event is not None:
            hbase_data.delete_from_expiration_index(id_hash, event.expiration)

        _move_event_to_hbase(id_hash, expiration, payload)

    return future_event(id_hash, payload, expiration)

def update_expiration(id_, expiration):
    now = datetime.datetime.utcnow()
    if expiration <= calendar.timegm(now.utctimetuple()):
        raise FesException("Expiration must be in the future.")

    id_hash = generate_hash(id_)

    storage_cutoff_time = now + datetime.timedelta(minutes=STORAGE_CUTOFF_MINUTES)

    #redis is faster, so check it first
    payload = redis_data.get_event_payload(id_hash)

    #it's already in redis
    if payload is not None:
        #the new expiration is within 15 minutes, just update redis expiration
        if expiration <= calendar.timegm(storage_cutoff_time.utctimetuple()):
            redis_data.update_expiration(id_hash, expiration)
        else:
            _move_event_to_hbase(id_hash, expiration, payload)
        return

    #retrieve event from hbase
    event = hbase_data.read_event(id_hash)

    #it's in hbase
    if event is not None:
        #the new expiration is not within 15 minutes, just update hbase expiration
        if expiration > calendar.timegm(storage_cutoff_time.utctimetuple()):
            hbase_data.delete_from_expiration_index(id_hash, event.expiration)
            hbase_data.add(id_hash, expiration, event.payload)
        else:
            move_event_to_redis(id_hash, expiration, event)
        return
    
    raise FesException("Event " + id_ + " not found.")

def update_event_payload(id_, payload):
    id_hash = generate_hash(id_)

    #redis is faster, so check it first
    result = redis_data.get_event_payload(id_hash)

    if result is not None:
        redis_data.update_event(id_hash, payload)
        return

    future_event = hbase_data.read_event(id_hash)

    if future_event is not None:
        hbase_data.write_event(id_hash, future_event.expiration, payload)
        return

    raise FesException("No event found for id " + id_)

def delete(id_):
    id_hash = generate_hash(id_)

    #delete from redis first
    event = redis_data.get_and_delete(id_hash)

    #if it wasn't in redis, delete from hbase
    if event is None:
        #retrieve expiration so we can delete the index entry
        event = hbase_data.read_event(id_hash)

        if event is not None:
            hbase_data.delete_all(id_hash, event.expiration)

def _move_event_to_hbase(id_hash, expiration, payload):
    if payload is None:
        raise FesException("Error copying event " + id_hash + " to hbase. Data not found.")

    redis_data.delete(id_hash)
    hbase_data.add(id_hash, expiration, payload)

def move_event_to_redis(id_hash, expiration, event):
    if event is None:
        raise FesException("Error copying event " + id_hash + " to redis. Data not found.")

    if expiration is None:
        expiration = event.expiration

    redis_data.add(id_hash, expiration, event.payload)
    hbase_data.delete_all(id_hash, event.expiration)
