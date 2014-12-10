import uuid
from random import randint
import datetime
import calendar

TEST_PAYLOAD = '{"a list" : ["a list item"], "a string" : "string value"}'

def get_random_string():
    return str(uuid.uuid4())

"""get a random 10 digit int"""
def get_random_int():
    return randint(1000000000, 9999999999)

def get_current_timestamp():
    return calendar.timegm(datetime.datetime.utcnow().utctimetuple())