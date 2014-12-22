from __future__ import print_function
import hbase_data
import fes_controller
import calendar
import time
from threading import Thread
import datetime
from FesException import FesException

SLEEP_TIME_SECONDS = 30

class MarshallingAgent(Thread):
    def __init__(self, scanner_prefix):
        self.scanner_prefix = scanner_prefix
        super(MarshallingAgent, self).__init__()

    def run(self):
        while True:
            id_hashes = _scan_for_hbase_expirations(self.scanner_prefix)

            #if there are no results, sleep for 30 seconds
            if id_hashes is None:
                time.sleep(SLEEP_TIME_SECONDS)
                continue

            for id_hash in id_hashes:
                try:
                    _marshall_event_into_redis(id_hash)
                except FesException as e:
                    print(str(e))

def _scan_for_hbase_expirations(scanner_prefix):
    start_time = calendar.timegm(datetime.datetime.utcnow().utctimetuple())

    start_row = scanner_prefix
    end_row = scanner_prefix + str(
        start_time + (fes_controller.STORAGE_CUTOFF_MINUTES * 60)
    )

    # scan our bucket from the beginning of time to one minute after the storage cutoff
    id_hashes = hbase_data.scan_expiration_index(start_row, end_row)

    return id_hashes

def _marshall_event_into_redis(id_hash):
    #read from hbase
    future_event = hbase_data.read_event(id_hash)

    if future_event is None:
        raise FesException("Failed to retrieve event from hbase hash_id=" + id_hash)
    else:
        fes_controller.move_event_to_redis(id_hash, None, future_event)
