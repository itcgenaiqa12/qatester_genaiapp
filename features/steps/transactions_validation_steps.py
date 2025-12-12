import os
import datetime
import uuid
from behave import when, then
import psycopg2
from decimal import Decimal

# Helper: read DB connection from environment variables (same pattern as other steps)
def get_conn():
    params = {
        'host': os.environ.get('PGHOST', 'localhost'),
        'port': int(os.environ.get('PGPORT', 5432)),
        'dbname': os.environ.get('PGDATABASE', 'postgres'),
        'user': os.environ.get('PGUSER', 'postgres'),
        'password': os.environ.get('PGPASSWORD', ''),
    }
    return psycopg2.connect(**params)

@when('inserting a transaction with currency = "{currency}"')
def step_insert_currency(context, currency):
    """Attempt to insert a single transaction with the provided currency code."""
    marker = str(uuid.uuid4())
    context.last_error = None
    context.insert_marker = marker
    try:
        conn = get_conn()
        with conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        "INSERT INTO sample_data.transactions (user_id, product_id, amount, currency, transaction_date, status, remarks, marker_tag) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                        (999999, 999999, Decimal('1.00'), currency, datetime.datetime.utcnow(), 'pending', 'validation-test', marker)
                    )
                except Exception as e:
                    context.last_error = e
    finally:
        try:
            conn.close()
        except Exception:
            pass

@then('the database should reject the insert due to CHAR(3) constraint')
def step_assert_currency_rejected(context):
    """Assert that the previous insert was rejected by the DB because currency exceeded CHAR(3)."""
    err = getattr(context, 'last_error', None)
    assert err is not None, 'Expected DB to reject insert but no error was raised'
    msg = str(err).lower()
    # common Postgres message for character(n) overflows contains "value too long" or "character"/"length"
    assert ('value too long' in msg) or ('character' in msg and 'length' in msg) or ('out of range' in msg) or ('truncate' in msg) , f"Unexpected DB error message for CHAR(3) violation: {err}"

@when('updating status to "{new_status}"')
def step_update_status(context, new_status):
    """Insert a fresh row and attempt to update its status to the given value. Record whether DB allowed it."""
    marker = str(uuid.uuid4())
    context.update_allowed = False
    context.updated_row_id = None
    context.qa_flagged = False
    try:
        conn = get_conn()
        with conn:
            with conn.cursor() as cur:
                # Insert baseline row
                cur.execute(
                    "INSERT INTO sample_data.transactions (user_id, product_id, amount, currency, transaction_date, status, remarks, marker_tag) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING transaction_id",
                    (111111, 222222, Decimal('5.00'), 'USD', datetime.datetime.utcnow(), 'pending', 'status-update-test', marker)
                )
                rowid = cur.fetchone()[0]
                # Attempt update
                cur.execute("UPDATE sample_data.transactions SET status = %s WHERE transaction_id = %s", (new_status, rowid))
                # Read back
                cur.execute("SELECT status FROM sample_data.transactions WHERE transaction_id = %s", (rowid,))
                status_db = cur.fetchone()[0]
                context.updated_row_id = rowid
                if status_db == new_status:
                    context.update_allowed = True
                # Simple QA rule: allowed business states
                allowed_states = {'pending', 'completed', 'failed', 'refund'}
                if status_db not in allowed_states:
                    context.qa_flagged = True
    finally:
        try:
            conn.close()
        except Exception:
            pass

@then('the database allows the update')
def step_assert_db_allowed_update(context):
    assert getattr(context, 'update_allowed', False), 'Expected DB to allow status update but it did not'

@then('the QA system flags it as violating business rules')
def step_assert_qa_flag(context):
    assert getattr(context, 'qa_flagged', False), 'Expected QA system to flag the updated status as a business rule violation'
