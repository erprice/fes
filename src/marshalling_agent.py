import redis_data
import hbase_data
import fes_controller
from future_event import future_event
import calendar
import time
import threading
import datetime

class marshalling_agent(threading.Thread):
    def __init__(self, scanner_prefix):
        self.scanner_prefix = scanner_prefix
        super(marshalling_agent, self).__init__()

    def run(self):
        while True:
            start_time = calendar.timegm(datetime.datetime.utcnow().utctimetuple())

            start_row = self.scanner_prefix
            end_row = self.scanner_prefix + str(start_time + (fes_controller.STORAGE_CUTOFF_MINUTES * 60))
            print "start_row=" + start_row
            print "end_row=" + end_row

            # scan our bucket from the beginning of time up to one minute in the future
            id_hashes = hbase_data.scan_expiration_index(start_row, end_row)

            #if there are no results, sleep for 30 seconds
            if id_hashes is None:
                time.sleep(30)
                continue

            for id_hash in id_hashes:
                print "marshalling event " + id_hash + " into redis"
                #read from hbase
                future_event = hbase_data.read_event(id_hash)

                #write to redis
                redis_data.add(id_hash, future_event.expiration, future_event.payload)

                #delete from hbase
                hbase_data.delete_all(id_hash, future_event.expiration)