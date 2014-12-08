import redis_data
import hbase_data
import fes_controller
from future_event import future_event
import calendar
import time
from threading import Thread
import datetime

SLEEP_TIME_SECONDS = 30

class marshalling_agent(Thread):
    def __init__(self, scanner_prefix):
        self.scanner_prefix = scanner_prefix
        super(marshalling_agent, self).__init__()

    def run(self):
        while True:
            start_time = calendar.timegm(datetime.datetime.utcnow().utctimetuple())

            start_row = self.scanner_prefix
            end_row = self.scanner_prefix + str(start_time + (fes_controller.STORAGE_CUTOFF_MINUTES * 60))

            # scan our bucket from the beginning of time up to one minute in the future
            id_hashes = hbase_data.scan_expiration_index(start_row, end_row)

            #if there are no results, sleep for 30 seconds
            if id_hashes is None:
                time.sleep(SLEEP_TIME_SECONDS)
                continue

            for id_hash in id_hashes:
                #read from hbase
                future_event = hbase_data.read_event(id_hash)

                if future_event is None:
                    print "ERROR failed to retrieve event from hbase hash_id=" + id_hash
                else:
                    #write to redis
                    redis_data.add(id_hash, future_event.expiration, future_event.payload)

                    #delete from hbase
                    hbase_data.delete_all(id_hash, future_event.expiration)