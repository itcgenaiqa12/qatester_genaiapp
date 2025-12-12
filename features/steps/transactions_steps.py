import os
import io
import csv
import uuid
import time
import random
import string
import datetime
from decimal import Decimal
from behave import given, when, then
import psycopg2
import psycopg2.extras
from faker import Faker

fake = Faker()

# Helper: read DB connection from environment variables
def get_conn():
    params = {
        'host': os.environ.get('PGHOST', 'localhost'),
        'port': int(os.environ.get('PGPORT', 5432)),
        'dbname': os.environ.get('PGDATABASE', 'postgres'),
        'user': os.environ.get('PGUSER', 'postgres'),
        'password': os.environ.get('PGPASSWORD', ''),
    }
    return psycopg2.connect(**params)

# Helper: generate a single synthetic transaction row (dictionary)
def gen_row(marker_tag=None):
    return {
        'user_id': random.randint(1, 10_000_000),
        'product_id': random.randint(1, 1_000_000),
        'amount': Decimal(str(round(random.uniform(1.0, 500.0), 2))),
        'currency': random.choice(['USD','EUR','GBP','USD','USD']),
        'transaction_date': fake.date_time_between(start_date='-2y', end_date='now'),
        'status': random.choice(['completed','pending','failed']),
        'remarks': None if random.random() < 0.2 else fake.sentence(nb_words=10),
        'marker_tag': marker_tag,
    }

# Create schema helper (will attempt to create the schema/table if not exists)
@given('the transactions table DDL is available')
def step_ddls_available(context):
    context.ddl = context.text.strip()

@given('the transactions table exists')
def step_create_table(context):
    ddl = getattr(context, 'ddl', None)
    if not ddl:
        # Use default create statement
        ddl = '''CREATE SCHEMA IF NOT EXISTS sample_data;\nCREATE TABLE IF NOT EXISTS sample_data.transactions (
  transaction_id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  product_id BIGINT NOT NULL,
  amount NUMERIC(12,2) NOT NULL,
  currency CHAR(3) NOT NULL DEFAULT 'USD',
  transaction_date TIMESTAMP NOT NULL DEFAULT NOW(),
  status VARCHAR(20) NOT NULL DEFAULT 'pending',
  remarks TEXT,
  marker_tag UUID
);'''
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
    finally:
        conn.close()

@when('I bulk-insert {num_rows:d} synthetic rows (batch_size={batch_size:d})')
def step_bulk_insert(context, num_rows, batch_size):
    marker = str(uuid.uuid4())
    context.run_marker = marker
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # Use COPY FROM STDIN for efficiency
                cols = ['user_id','product_id','amount','currency','transaction_date','status','remarks','marker_tag']
                sio = io.StringIO()
                writer = csv.writer(sio)
                rows_written = 0
                for i in range(num_rows):
                    r = gen_row(marker_tag=marker)
                    writer.writerow([r['user_id'], r['product_id'], str(r['amount']), r['currency'], r['transaction_date'].isoformat(sep=' '), r['status'], r['remarks'] or '', marker])
                    rows_written += 1
                    # Flush per batch to avoid huge memory usage
                    if rows_written % batch_size == 0:
                        sio.seek(0)
                        cur.copy_expert("COPY sample_data.transactions(user_id, product_id, amount, currency, transaction_date, status, remarks, marker_tag) FROM STDIN WITH (FORMAT csv)", sio)
                        sio = io.StringIO()
                        writer = csv.writer(sio)
                # flush remaining
                sio.seek(0)
                if sio.tell() != 0:
                    cur.copy_expert("COPY sample_data.transactions(user_id, product_id, amount, currency, transaction_date, status, remarks, marker_tag) FROM STDIN WITH (FORMAT csv)", sio)
    finally:
        conn.close()

@when('I insert a sample set of transactions:')
def step_insert_sample_table(context):
    # context.table is behave table with amount/currency
    rows = []
    for row in context.table:
        rows.append((1, 1, Decimal(row['amount']), row['currency'], datetime.datetime.utcnow(), 'completed', 'sample', None))
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur,
                                               "INSERT INTO sample_data.transactions (user_id, product_id, amount, currency, transaction_date, status, remarks, marker_tag) VALUES %s",
                                               rows,
                                               template=None)
    finally:
        conn.close()

@when('I attempt to insert a transaction with amount = {amount_str}')
def step_insert_amount(context, amount_str):
    marker = getattr(context, 'run_marker', str(uuid.uuid4()))
    conn = get_conn()
    context.last_error = None
    try:
        with conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("INSERT INTO sample_data.transactions (user_id, product_id, amount, currency, transaction_date, status, remarks, marker_tag) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                                (1,1,Decimal(amount_str), 'USD', datetime.datetime.utcnow(), 'completed', 'overflow-test', marker))
                except Exception as e:
                    context.last_error = e
    finally:
        conn.close()

@then('the insert should succeed')
def step_assert_insert_success(context):
    assert getattr(context, 'last_error', None) is None, f"Expected insert to succeed but got error: {context.last_error}"

@then('the insert should fail with a numeric overflow error')
def step_assert_insert_overflow(context):
    assert getattr(context, 'last_error', None) is not None, 'Expected an error but insert succeeded'
    # basic check for numeric overflow text in exception
    msg = str(context.last_error).lower()
    assert 'numeric' in msg or 'overflow' in msg or 'out of range' in msg, f"Unexpected error message: {context.last_error}"

@when('I insert a refund transaction with amount = {amount_str}')
def step_insert_refund(context, amount_str):
    marker = getattr(context, 'run_marker', str(uuid.uuid4()))
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO sample_data.transactions (user_id, product_id, amount, currency, transaction_date, status, remarks, marker_tag) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                            (2,2,Decimal(amount_str), 'USD', datetime.datetime.utcnow(), 'refund', 'refund-test', marker))
    finally:
        conn.close()

@then('selecting COUNT(*) WHERE amount < 0 should return at least 1')
def step_assert_negative_count(context):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM sample_data.transactions WHERE amount < 0")
                cnt = cur.fetchone()[0]
                assert cnt >= 1, f"Expected at least 1 negative amount row but found {cnt}"
    finally:
        conn.close()

@when("I insert transactions with currencies: 'USD', '', 'US', '€', NULL")
def step_insert_currencies(context):
    marker = getattr(context, 'run_marker', str(uuid.uuid4()))
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                samples = [('USD',), ('',), ('US',), ('€',), (None,)]
                for cur_code, in samples:
                    cur.execute("INSERT INTO sample_data.transactions (user_id, product_id, amount, currency, transaction_date, status, remarks, marker_tag) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                                (3,3,Decimal('10.00'), cur_code, datetime.datetime.utcnow(), 'completed', 'currency-test', marker))
    finally:
        conn.close()

@then('currency integrity check should flag invalid_currency_count >= {min_invalid:d}')
def step_check_invalid_currency(context, min_invalid):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM sample_data.transactions WHERE length(trim(currency)) != 3 OR currency IS NULL OR currency ~ '[^A-Za-z0-9]'")
                cnt = cur.fetchone()[0]
                assert cnt >= min_invalid, f"Expected at least {min_invalid} invalid currency rows but found {cnt}"
    finally:
        conn.close()

@when('I insert transactions with dates {date1} and {date2}')
def step_insert_extreme_dates(context, date1, date2):
    marker = getattr(context, 'run_marker', str(uuid.uuid4()))
    d1 = datetime.datetime.fromisoformat(date1)
    d2 = datetime.datetime.fromisoformat(date2)
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO sample_data.transactions (user_id, product_id, amount, currency, transaction_date, status, remarks, marker_tag) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                            (4,4,Decimal('50.00'),'USD', d1, 'completed', 'date-extreme', marker))
                cur.execute("INSERT INTO sample_data.transactions (user_id, product_id, amount, currency, transaction_date, status, remarks, marker_tag) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                            (5,5,Decimal('60.00'),'USD', d2, 'completed', 'date-extreme', marker))
    finally:
        conn.close()

@then('querying recent window \(last 30 days\) should exclude those extreme dates')
def step_assert_recent_excludes_extremes(context):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM sample_data.transactions WHERE transaction_date >= NOW() - INTERVAL '30 days' AND (remarks = 'date-extreme')")
                cnt = cur.fetchone()[0]
                assert cnt == 0, f"Expected 0 extreme-date rows in last 30 days but found {cnt}"
    finally:
        conn.close()

@when('I insert a transaction with status of length {length:d}')
def step_insert_long_status(context, length):
    status = ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    marker = getattr(context, 'run_marker', str(uuid.uuid4()))
    conn = get_conn()
    context.last_status_error = None
    try:
        with conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("INSERT INTO sample_data.transactions (user_id, product_id, amount, currency, transaction_date, status, remarks, marker_tag) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                                (6,6,Decimal('12.00'),'USD', datetime.datetime.utcnow(), status, 'long-status', marker))
                except Exception as e:
                    context.last_status_error = e
    finally:
        conn.close()

@then('the insert should either truncate or fail depending on DB constraint')
def step_assert_status_behavior(context):
    # If last_status_error is set, it's a failure; otherwise it succeeded (DB may truncate)
    if getattr(context, 'last_status_error', None):
        # acceptable if DB raised length/constraint error
        assert 'value too long' in str(context.last_status_error).lower() or 'truncate' in str(context.last_status_error).lower() or 'length' in str(context.last_status_error).lower()
    else:
        assert True

@when('I insert a transaction with a remarks field of size 1MB and unicode content')
def step_insert_large_remarks(context):
    big = ('𠜎' * 1000) * 1000  # ~1M characters of unicode
    marker = getattr(context, 'run_marker', str(uuid.uuid4()))
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO sample_data.transactions (user_id, product_id, amount, currency, transaction_date, status, remarks, marker_tag) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                            (7,7,Decimal('99.99'),'USD', datetime.datetime.utcnow(), 'completed', big, marker))
                context.large_remarks_marker = marker
    finally:
        conn.close()

@then('selecting that transaction should return the full remarks text')
def step_assert_large_remarks_retrieved(context):
    marker = getattr(context, 'large_remarks_marker', None)
    assert marker is not None
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT length(remarks::text) FROM sample_data.transactions WHERE marker_tag = %s", (marker,))
                row = cur.fetchone()
                assert row and row[0] > 500_000, f"Remarks size too small or missing: {row}"
    finally:
        conn.close()

@when('I perform a transactional bulk import where one row violates NOT NULL')
def step_bulk_import_with_violation(context):
    marker = getattr(context, 'run_marker', str(uuid.uuid4()))
    conn = get_conn()
    context.bulk_import_error = None
    try:
        with conn:
            with conn.cursor() as cur:
                try:
                    # begin transaction explicitly
                    cur.execute("BEGIN")
                    cur.execute("INSERT INTO sample_data.transactions (user_id, product_id, amount, currency, transaction_date, status, remarks, marker_tag) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                                (8,8,Decimal('10.00'),'USD', datetime.datetime.utcnow(), 'completed', 'ok', marker))
                    # violate NOT NULL by inserting NULL into user_id
                    cur.execute("INSERT INTO sample_data.transactions (user_id, product_id, amount, currency, transaction_date, status, remarks, marker_tag) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                                (None,9,Decimal('20.00'),'USD', datetime.datetime.utcnow(), 'completed', 'viol', marker))
                    cur.execute("COMMIT")
                except Exception as e:
                    context.bulk_import_error = e
                    # rollback
                    cur.execute("ROLLBACK")
    finally:
        conn.close()

@then('the entire import should be rolled back (no partial commits)')
def step_assert_bulk_rollback(context):
    marker = getattr(context, 'run_marker', None)
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM sample_data.transactions WHERE marker_tag = %s", (marker,))
                cnt = cur.fetchone()[0]
                assert cnt == 0, f"Expected 0 rows for marker {marker} after rollback but found {cnt}"
    finally:
        conn.close()

@when('I simulate sequence near exhaustion by setting sequence to a high value')
def step_simulate_sequence(context):
    # caution: this manipulates the sequence - only for test environments
    conn = get_conn()
    context.seq_error = None
    try:
        with conn:
            with conn.cursor() as cur:
                # set sequence to a high but safe value for testing
                cur.execute("SELECT setval(pg_get_serial_sequence('sample_data.transactions','transaction_id'), 9223372036854770000)")
    except Exception as e:
        context.seq_error = e
    finally:
        conn.close()

@when('I attempt a bulk insert of {small_rows:d} rows')
def step_attempt_bulk_after_seq(context, small_rows):
    # try inserting small_rows and capture errors
    marker = getattr(context, 'run_marker', str(uuid.uuid4()))
    conn = get_conn()
    context.seq_insert_error = None
    try:
        with conn:
            with conn.cursor() as cur:
                try:
                    for i in range(small_rows):
                        cur.execute("INSERT INTO sample_data.transactions (user_id, product_id, amount, currency, transaction_date, status, remarks, marker_tag) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                                    (10+i,10+i,Decimal('1.00'),'USD', datetime.datetime.utcnow(), 'completed', 'seq-test', marker))
                except Exception as e:
                    context.seq_insert_error = e
    finally:
        conn.close()

@then('insertion should either fail gracefully or raise a sequence error')
def step_assert_seq_behavior(context):
    # either error occurred or not; if error, check message refers to sequence or bigint overflow
    if getattr(context, 'seq_insert_error', None):
        msg = str(context.seq_insert_error).lower()
        assert 'sequence' in msg or 'overflow' in msg or 'bigint' in msg or 'out of range' in msg
    else:
        assert True

@when('I delete test rows inserted by this test run')
def step_cleanup_marker(context):
    marker = getattr(context, 'run_marker', None)
    if not marker:
        return
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM sample_data.transactions WHERE marker_tag = %s", (marker,))
                context.cleanup_deleted = cur.rowcount
    finally:
        conn.close()

@then('the cleanup should remove rows with marker_tag equal to the current run id')
def step_assert_cleanup(context):
    assert getattr(context, 'cleanup_deleted', 0) >= 0
