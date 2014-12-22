import redis
from FutureEvent import FutureEvent

EXPIRATION_QUEUE = "fes:expiration_queue"
EVENT_QUEUE = "fes:event_queue"

REDIS_SERVER = redis.Redis("localhost")

def add(id_hash, expiration, payload):
    pipe = REDIS_SERVER.pipeline()
    pipe.zadd(EXPIRATION_QUEUE, id_hash, expiration)
    pipe.hset(EVENT_QUEUE, id_hash, payload)
    pipe.execute()

def get_expiration(id_hash):
    return REDIS_SERVER.zscore(EXPIRATION_QUEUE, id_hash)

def get_event_payload(id_hash):
    return REDIS_SERVER.hget(EVENT_QUEUE, id_hash)

def update_expiration(id_hash, expiration):
    REDIS_SERVER.zadd(EXPIRATION_QUEUE, id_hash, expiration)

def update_event(id_hash, payload):
    REDIS_SERVER.hset(EVENT_QUEUE, id_hash, payload)

def delete(id_hash):
    pipe = REDIS_SERVER.pipeline()
    pipe.zrem(EXPIRATION_QUEUE, id_hash)
    pipe.hdel(EVENT_QUEUE, id_hash)
    pipe.execute()

def get_and_delete(id_hash):
    pipe = REDIS_SERVER.pipeline()
    pipe.hget(EVENT_QUEUE, id_hash)
    pipe.zscore(EXPIRATION_QUEUE, id_hash)
    pipe.hdel(EVENT_QUEUE, id_hash)
    pipe.zrem(EXPIRATION_QUEUE, id_hash)
    redis_response = pipe.execute()

    if redis_response[0] is None:
        return None
    else:
        return FutureEvent(None, redis_response[0], int(redis_response[1]))

def get_expiration_range(start_time, end_time):
    """
    Retrieve a list of all events expiring before a specified date time epoch.
    start_time and end_time should be a 10-digit unix time epoch or "-inf" or "+inf"
    """
    result = REDIS_SERVER.zrangebyscore(EXPIRATION_QUEUE, start_time, end_time, None, None, True)
    if not result:
        return None
    else:
        return result
