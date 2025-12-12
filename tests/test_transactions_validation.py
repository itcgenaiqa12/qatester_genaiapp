import os
import datetime
import uuid
import pytest
from decimal import Decimal
import psycopg2

# Helper to build DB connection
def get_conn():
    params = {
        'host': os.environ.get('PGHOST', 'localhost'),
        'port': int(os.environ.get('PGPORT', 5432)),
        'dbname': os.environ.get('PGDATABASE', 'postgres'),
        'user': os.environ.get('PGUSER', 'postgres'),
        'password': os.environ.get('PGPASSWORD', ''),
    }
    return psycopg2.connect(**params)

@pytest.fixture(scope='module')
def conn():
    c = get_conn()
    yield c
    c.close()


def test_currency_char3_rejects(conn):
    marker = str(uuid.uuid4())
    err = None
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO sample_data.transactions (user_id, product_id, amount, currency, transaction_date, status, remarks, marker_tag) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                            (10001,10002, Decimal('1.00'), 'USDX', datetime.datetime.utcnow(), 'pending', 'pytest-currency-test', marker))
    except Exception as e:
        err = e
    assert err is not None, "Expected DB to reject currency with length > 3"
    msg = str(err).lower()
    assert ('value too long' in msg) or ('character' in msg and 'length' in msg) or ('truncate' in msg) or ('out of range' in msg)


def test_status_update_unexpected_value(conn):
    marker = str(uuid.uuid4())
    rowid = None
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO sample_data.transactions (user_id, product_id, amount, currency, transaction_date, status, remarks, marker_tag) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING transaction_id",
                            (20001,20002, Decimal('5.00'), 'USD', datetime.datetime.utcnow(), 'pending', 'pytest-status-test', marker))
                rowid = cur.fetchone()[0]
                cur.execute("UPDATE sample_data.transactions SET status = %s WHERE transaction_id = %s", ('invalid_state', rowid))
                cur.execute("SELECT status FROM sample_data.transactions WHERE transaction_id = %s", (rowid,))
                status_db = cur.fetchone()[0]
    finally:
        # cleanup
        if rowid:
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM sample_data.transactions WHERE transaction_id = %s", (rowid,))
            except Exception:
                pass
    assert status_db == 'invalid_state', "DB did not allow updating to unexpected status"
    allowed_states = {'pending','completed','failed','refund'}
    assert status_db not in allowed_states, "QA should flag non-standard status" 
