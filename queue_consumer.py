from redis_controller import redis_controller
from future_event import future_event
import calendar
import time
import threading
import requests

class queue_consumer(threading.Thread):
    def __init__(self):
        super(queue_consumer, self).__init__()

    def run(self):
        while True:
            now = calendar.timegm(datetime.datetime.utcnow().utctimetuple())
            event_ids = redis_controller.get_expiration_range("-inf", now)
            
            #if there is no work to do, sleep for 1 second
            if event_ids is None:
                time.sleep(1)
            else:
                for tuple in event_ids:
                    future_event = redis_controller.get_and_delete(tuple[0])
                    consumer_response = requests.put(FES_CONSUMER_URL, data=future_event.payload, 
                        headers={'Content-Type': 'application/json'})

                    #TODO delete me
                    print "expiring event: " + future_event.payload