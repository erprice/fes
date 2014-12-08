import hbase_data

def test_generate_salted_rowkey():
    rowkey = "asdf"
    expiration = 123456
    salted_rowkey = "a_123456"

    assert hbase_data._generate_salted_row_key(rowkey, expiration) == salted_rowkey