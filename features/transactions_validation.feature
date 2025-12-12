Feature: Transactions data validation and business rule checks
  As a QA/security engineer
  I want to validate DB constraints and business-rule enforcement for transactions
  So that invalid data is rejected or flagged appropriately

  Background:
    Given the transactions table exists

  Scenario: Insert transaction with invalid currency length
    When inserting a transaction with currency = "USDX"
    Then the database should reject the insert due to CHAR(3) constraint

  Scenario: Update status field with unexpected value
    When updating status to "invalid_state"
    Then the database allows the update
    But the QA system flags it as violating business rules
