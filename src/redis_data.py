import redis
from future_event import future_event

EXPIRATION_QUEUE = "fes:expiration_queue"
EVENT_QUEUE = "fes:event_queue"

redis_server = redis.Redis("localhost")

def add(id_hash, expiration, payload):
    pipe = redis_server.pipeline()
    pipe.zadd(EXPIRATION_QUEUE, id_hash, expiration)
    pipe.hset(EVENT_QUEUE, id_hash, payload)
    pipe.execute()

def get_expiration(id_hash):
    return redis_server.zscore(EXPIRATION_QUEUE, id_hash)

def get_event_payload(id_hash):
    return redis_server.hget(EVENT_QUEUE, id_hash)

def update_expiration(id_hash, expiration):
    redis_server.zadd(EXPIRATION_QUEUE, id_hash, expiration)

def update_event(id_hash, payload):
    redis_server.hset(EVENT_QUEUE, id_hash, payload)

def delete(id_hash):
    pipe = redis_server.pipeline()
    pipe.zrem(EXPIRATION_QUEUE, id_hash)
    pipe.hdel(EVENT_QUEUE, id_hash)
    pipe.execute()

def get_and_delete(id_hash):
    pipe = redis_server.pipeline()
    pipe.hget(EVENT_QUEUE, id_hash)
    pipe.zscore(EXPIRATION_QUEUE, id_hash)
    pipe.hdel(EVENT_QUEUE, id_hash)
    pipe.zrem(EXPIRATION_QUEUE, id_hash)
    redis_response = pipe.execute()

    if redis_response[0] is None:
        return None
    else:
        return future_event(None, redis_response[0], int(redis_response[1]))

"""
Retrieve a list of all events expiring before a specified date time epoch.
start_time and end_time should be a 10-digit unix time epoch or "-inf" or "+inf" 
"""
def get_expiration_range(start_time, end_time):
    result = redis_server.zrangebyscore(EXPIRATION_QUEUE, start_time, end_time, None, None, True)
    if not result:
        return None
    else:
        return result
