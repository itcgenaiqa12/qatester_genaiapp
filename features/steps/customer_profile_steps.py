import json
import re
from behave import given, when, then

# Simple rule-based generator used by tests
def parse_days_ago(text):
    if not text:
        return None
    m = re.match(r"(\d+)\s+days\s+ago", text)
    if m:
        return int(m.group(1))
    return None


def generate_profile(data):
    profile = {}
    aov = data.get("aov")
    ltv = data.get("ltv_segment")
    if not ltv:
        if aov is None:
            profile["ltv_segment"] = "Unknown"
        elif aov >= 150:
            profile["ltv_segment"] = "High"
        elif aov >= 75:
            profile["ltv_segment"] = "Medium"
        else:
            profile["ltv_segment"] = "Low"
    else:
        profile["ltv_segment"] = ltv

    ds = (data.get("discount_sensitivity") or "").lower()
    if "high" in ds or (isinstance(ds, str) and ">" in ds):
        profile["discount_sensitive"] = True
    else:
        profile["discount_sensitive"] = False

    # recommendations: simple rule for camping tents
    profile["recommendations"] = []
    for b in data.get("browsing", []):
        if "camp" in b.get("item", "").lower() or "tent" in b.get("item", "").lower():
            if b.get("count", 0) > 0:
                profile["recommendations"].append("Camping Tents")

    # loyalty flag if single brand affinity is present and only one brand
    brands = data.get("brand_affinity", []) or []
    profile["loyalty_flag"] = bool(brands) and len(brands) == 1

    # churn risk: last purchase > 180 days => true
    last_purchase = data.get("last_purchase")
    days = None
    if isinstance(last_purchase, str):
        days = parse_days_ago(last_purchase)
    if days is None and last_purchase is None and data.get("purchases"):
        profile["churn_risk"] = False
    elif days is None and last_purchase is None:
        profile["churn_risk"] = True
    else:
        profile["churn_risk"] = (days is not None and days > 180)

    # cross-sell: boots + tents browsing
    names = [p.get("name", "").lower() for p in data.get("purchases", [])]
    profile["cross_sell_opportunity"] = (any("boot" in n for n in names) or any("hiking" in n for n in names)) and any("camp" in b.get("item", "").lower() for b in data.get("browsing", []))

    # needs_more_data if critical fields missing
    critical = ["purchases", "last_purchase", "aov", "browsing", "brand_affinity", "discount_sensitivity"]
    profile["needs_more_data"] = any(data.get(k) in (None, [], "") for k in critical)

    return profile

@given('the customer data:')
def step_given_customer_data(context):
    context.input_data = json.loads(context.text)

@when('I generate the customer profile')
def step_generate_profile(context):
    context.profile = generate_profile(context.input_data)

@then('the profile LTV segment should be "{expected}"')
def step_assert_ltv(context, expected):
    assert context.profile.get("ltv_segment") == expected, f"ltv_segment {context.profile.get('ltv_segment')} != {expected}"

@then('discount_sensitive should be {expected}')
def step_assert_discount(context, expected):
    exp = True if expected.lower() == "true" else False
    assert context.profile.get("discount_sensitive") == exp, f"discount_sensitive {context.profile.get('discount_sensitive')} != {exp}"

@then('recommendations should include "{item}"')
def step_assert_recommendation(context, item):
    assert item in context.profile.get("recommendations", []), f"{item} not in {context.profile.get('recommendations')}"

@then('loyalty_flag should be {expected}')
def step_assert_loyalty(context, expected):
    exp = True if expected.lower() == "true" else False
    assert context.profile.get("loyalty_flag") == exp, f"loyalty_flag {context.profile.get('loyalty_flag')} != {exp}"

@then('churn_risk should be {expected}')
def step_assert_churn(context, expected):
    exp = True if expected.lower() == "true" else False
    assert context.profile.get("churn_risk") == exp, f"churn_risk {context.profile.get('churn_risk')} != {exp}"
