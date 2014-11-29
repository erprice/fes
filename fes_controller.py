from future_event import future_event
import hbase_data
import redis_data
import hashlib

def generate_hash(id):
    return hashlib.sha224(id).hexdigest()

def move_event_to_hbase(id_hash, expiration):
    #get event from redis and delete both entries
    future_event = redis_data.get_and_delete(id_hash)

    #TODO better exception handling
    if future_event is None:
        print "ERROR copying event " + id_hash + " to hbase, data not found"
        return

    if expiration is None:
        expiration = future_event.expiration

    #add both entries to hbase
    hbase_data.write_hbase_event(id_hash, expiration, future_event.payload)
    hbase_data.write_hbase_expiration_index(id_hash, expiration)

def move_event_to_redis(id_hash, expiration):
    #get event from hbase
    event = hbase_data.read_hbase_event(id_hash)

    #TODO better exception handling
    if event is None:
        print "ERROR copying event " + id_hash + " to redis, data not found"
        return

    #delete both entries from hbase
    hbase_data.delete_hbase_event(id_hash)
    hbase_data.delete_hbase_expiration(id_hash, event.expiration)

    if expiration is not None:
        event.expiration = expiration

    redis_data.add(id_hash, expiration, event.payload)