# DDL reference is used to create the table when running tests
Feature: Transactions table edge cases for very large datasets
  In order to validate correctness and resilience when the `transactions` table contains more than 10 million rows
  As a DBA / QA engineer
  I want Gherkin scenarios that exercise functional, validation, boundary and operational edge-cases

  Background: schema
    Given the database contains the transactions table DDL:
      """
      CREATE TABLE sample_data.transactions (
          transaction_id BIGSERIAL PRIMARY KEY,
          user_id BIGINT NOT NULL,
          product_id BIGINT NOT NULL,
          amount NUMERIC(12,2) NOT NULL,
          currency CHAR(3) NOT NULL DEFAULT 'USD',
          transaction_date TIMESTAMP NOT NULL DEFAULT NOW(),
          status VARCHAR(20) NOT NULL DEFAULT 'pending',
          remarks TEXT
      );
      """

  # Functional / scale
  Scenario: Table row-count sanity when dataset exceeds 10 million
    Given the transactions table exists and is populated with >10000000 rows
    When I query "SELECT COUNT(*) FROM sample_data.transactions;"
    Then the returned count should be > 10000000

  Scenario: Aggregation correctness at scale (SUM/AVG)
    Given the transactions table exists with representative distribution
    When I run "SELECT SUM(amount)::numeric(20,2), AVG(amount)::numeric(12,2) FROM sample_data.transactions WHERE currency='USD';"
    Then the numeric aggregation results should be within acceptable precision and match expected baselines

  # Validation / boundary
  Scenario: Numeric precision boundary and overflow handling
    Given the transactions table exists
    When I insert a transaction with amount = 9999999999.99
    Then the insert should succeed
    When I insert a transaction with amount = 10000000000.00
    Then the insert should fail with numeric overflow or out-of-range error

  Scenario: Negative amounts and refund processing
    Given the transactions table exists
    When I insert refund transactions (amount < 0)
    Then refund rows should be queryable and counted by downstream rules

  Scenario: Currency code validation and malformed values
    Given the transactions table exists
    When I insert transactions with currencies: 'USD', '', 'US', '€', NULL
    Then an integrity check should flag invalid or nonconforming currencies

  Scenario: Transaction_date extreme values excluded from recent windows
    Given the transactions table exists
    When I insert transactions with transaction_date = '1900-01-01' and '2099-12-31'
    Then queries constrained to the last 30 days should exclude those extreme records

  Scenario: Status length and truncation/constraint behavior
    Given the transactions table exists
    When I attempt to insert a row with a status string longer than 20 characters
    Then the DB should either reject with an error or truncate per schema/DB settings

  Scenario: Remarks large payload and unicode support
    Given the transactions table exists
    When I insert a transaction with a remarks payload >= 1MB containing multi-byte unicode
    Then selecting that row should return the full remarks content without corruption

  # Operational / resilience
  Scenario: Bulk import atomicity (transactional rollback on violation)
    Given the transactions table exists
    When I perform a transactional bulk import where one row violates a NOT NULL constraint
    Then the entire import should be rolled back and no partial rows should persist

  Scenario: Sequence exhaustion simulation and graceful failure
    Given the transactions table exists
    When I set the transaction_id sequence near BIGINT max and attempt bulk inserts
    Then inserts should fail with understandable errors or the system should handle sequence rotation per policy

  Scenario: Concurrent writes and isolation under heavy load
    Given the transactions table exists and multiple clients are writing concurrently
    When a high-concurrency workload is applied (multi-client inserts/updates)
    Then no lost updates or data corruption should occur and deadlocks should be handled

  Scenario: Pagination determinism with duplicate sort keys
    Given the table contains >10M rows with many equal transaction_date values
    When paginating by transaction_date DESC LIMIT/OFFSET or keyset pagination
    Then pagination must be stable (no missing or duplicate rows across pages)

  Scenario: Index statistics, planner and ANALYZE after bulk load
    Given the transactions table exists and a large bulk load was performed
    When queries become slow due to stale statistics
    Then running ANALYZE should restore appropriate planner choices and improve performance

  Scenario: Partitioning, archival and historical queries
    Given the transactions table is partitioned by month (optional)
    When older partitions are archived to cheaper storage
    Then queries spanning active + archived ranges should return consistent results and meet SLA for historical reads

  Scenario: VACUUM, bloat and long-running deletes
    Given a delete of millions of rows is executed
    When VACUUM/auto-vacuum runs or manual maintenance is executed
    Then table bloat should be reclaimed and read performance restored within expected window

  Scenario: Backup & restore integrity for very large datasets
    Given a full backup is taken when the table contains >10M rows
    When the backup is restored to a test instance
    Then row counts and sample checksums should match the original source for validated partitions

  Scenario: Deduplication and idempotent ingestion
    Given ingestion can retry and produce duplicates based on business keys
    When deduplication logic runs (e.g., upsert using unique business key)
    Then duplicates are removed and only unique events remain

  Scenario: Monitoring, alerts and autoscaling on ingestion spikes
    Given ingestion rate spikes beyond operational thresholds (>1M rows/hour)
    When automatic throttling or scale-up is executed
    Then monitoring alerts should trigger and backpressure should protect system stability

  Scenario Outline: Representative query SLA under heavy load
    Given the transactions table contains >10000000 rows and the system is under load
    When I run "<query>"
    Then the query should complete within <timeout_seconds> seconds or return a controlled timeout error

    Examples:
      | query                                                            | timeout_seconds |
      | SELECT COUNT(*) FROM sample_data.transactions;                   | 30              |
      | SELECT SUM(amount) FROM sample_data.transactions;                | 30              |
      | SELECT * FROM sample_data.transactions ORDER BY transaction_date DESC LIMIT 1000; | 10 |

  # Implementation notes: step definitions for these scenarios should
  # - prepare synthetic datasets (COPY FROM or batched inserts),
  # - use transactions for atomicity tests, simulate failures for rollback,
  # - use setval to simulate sequence edge cases (only in test env),
  # - capture query timings and compare against SLA thresholds,
  # - ensure tests run only in non-production environments.
