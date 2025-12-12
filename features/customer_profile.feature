Feature: Build customer profile from purchase and behavior data
  As a personalization engine
  I want to convert customer history into a profile with segments, signals and recommendations
  So that downstream systems can target/offers correctly

  Scenario: High-value recent shopper who browsed camping tents
    Given the customer data:
      """
      {
        "purchases": [{"sku":"SKU-A","name":"Hiking Boots","date":"30 days ago"}],
        "last_purchase":"30 days ago",
        "aov":185,
        "browsing":[{"item":"Camping Tents","count":3,"window_days":7}],
        "brand_affinity":["NorthFace","Osprey"],
        "discount_sensitivity":"High"
      }
      """
    When I generate the customer profile
    Then the profile LTV segment should be "High"
    And discount_sensitive should be true
    And recommendations should include "Camping Tents"

  Scenario: High-value loyal customer with no recent browsing
    Given the customer data:
      """
      {
        "purchases":[{"sku":"SKU-C","name":"Backpack","date":"10 days ago"}],
        "last_purchase":"10 days ago",
        "aov":210,
        "browsing":[],
        "brand_affinity":["NorthFace"],
        "discount_sensitivity":"Low"
      }
      """
    When I generate the customer profile
    Then the profile LTV segment should be "High"
    And discount_sensitive should be false
    And loyalty_flag should be true

  Scenario: Bargain hunter low-value shopper
    Given the customer data:
      """
      {
        "purchases":[{"sku":"SKU-D","name":"T-Shirt","date":"15 days ago"}],
        "last_purchase":"15 days ago",
        "aov":25,
        "browsing":[{"item":"Sale Shoes","count":4,"window_days":14}],
        "brand_affinity":["Generic"],
        "discount_sensitivity":"High"
      }
      """
    When I generate the customer profile
    Then the profile LTV segment should be "Low"
    And discount_sensitive should be true

  Scenario: New customer who only browsed multiple categories
    Given the customer data:
      """
      {
        "purchases":[],
        "last_purchase":null,
        "aov":null,
        "browsing":[{"item":"Camping Tents","count":2,"window_days":7},{"item":"Hiking Boots","count":1,"window_days":7}],
        "brand_affinity":[],
        "discount_sensitivity":"Medium"
      }
      """
    When I generate the customer profile
    Then the profile should indicate "needs_more_data"

  Scenario: Churn risk for formerly loyal customer
    Given the customer data:
      """
      {
        "purchases":[{"sku":"SKU-E","name":"Jacket","date":"365 days ago"}],
        "last_purchase":"365 days ago",
        "aov":140,
        "browsing":[],
        "brand_affinity":["NorthFace"],
        "discount_sensitivity":"Low"
      }
      """
    When I generate the customer profile
    Then churn_risk should be true
    And loyalty_flag should be true
