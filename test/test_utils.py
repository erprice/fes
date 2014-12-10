import uuid
from random import randint
import datetime
import calendar
import hbase_data

TEST_PAYLOAD = '{"a list" : ["a list item"], "a string" : "string value"}'

def get_random_string():
    return str(uuid.uuid4())

"""get a random 10 digit int"""
def get_random_int():
    return randint(1000000000, 9999999999)

def get_current_timestamp():
    return calendar.timegm(datetime.datetime.utcnow().utctimetuple())

def scan_index(id_hash, expiration):
    start_row = hbase_data._generate_salted_row_key(id_hash, expiration)
    end_row = start_row

    id_hashes = hbase_data.scan_expiration_index(start_row, end_row)
    return id_hashes