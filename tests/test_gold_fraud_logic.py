import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

import gold

TXN_SCHEMA = StructType([
    StructField("account_id", StringType()),
    StructField("txn_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("is_international", BooleanType()),
    StructField("kyc_status", StringType()),
    StructField("customer_name", StringType()),
])

WATCHLIST_SCHEMA = StructType([
    StructField("account_id", StringType()),
    StructField("fraud_type", StringType()),
])


def make_txn_df(spark, rows):
    return spark.createDataFrame(rows, TXN_SCHEMA)


def make_watchlist_df(spark, rows):
    return spark.createDataFrame(rows, WATCHLIST_SCHEMA)


def test_watchlisted_account_is_confirmed_fraud(spark, test_config):
    txns = make_txn_df(spark, [
        ("ACC-001", "TXN-1", 500.0, False, "verified", "Alice"),
    ])
    watchlist = make_watchlist_df(spark, [("ACC-001", "card_cloning")])

    result, _ = gold.apply_fraud_flags(txns, watchlist, test_config)
    row = result.collect()[0]

    assert row["is_watchlisted"] is True
    assert row["fraud_risk_level"] == "confirmed_fraud"


def test_confirmed_fraud_wins_even_if_also_suspicious(spark, test_config):
    # watchlisted AND high-value international AND risky kyc -> still confirmed_fraud, not suspicious
    txns = make_txn_df(spark, [
        ("ACC-002", "TXN-2", 100.0, False, "verified", "Bob"),
        ("ACC-002", "TXN-3", 999999.0, True, "rejected", "Bob"),
    ])
    watchlist = make_watchlist_df(spark, [("ACC-002", "money_laundering")])

    result, _ = gold.apply_fraud_flags(txns, watchlist, test_config)
    levels = {row["txn_id"]: row["fraud_risk_level"] for row in result.collect()}

    assert levels["TXN-3"] == "confirmed_fraud"


def test_high_value_international_flags_suspicious(spark, test_config):
    txns = make_txn_df(spark, [
        ("ACC-010", "TXN-10", 100.0, True, "verified", "Carl"),
        ("ACC-011", "TXN-11", 200.0, True, "verified", "Dave"),
        ("ACC-012", "TXN-12", 100000.0, True, "verified", "Erin"),  # should be high value
    ])
    watchlist = make_watchlist_df(spark, [])

    result, threshold = gold.apply_fraud_flags(txns, watchlist, test_config)
    row = result.filter(result.txn_id == "TXN-12").collect()[0]

    assert row["is_high_value_international"] is True
    assert row["fraud_risk_level"] == "suspicious"


def test_high_value_but_domestic_is_not_flagged_for_that_signal(spark, test_config):
    txns = make_txn_df(spark, [
        ("ACC-020", "TXN-20", 100.0, True, "verified", "Fay"),
        ("ACC-021", "TXN-21", 999999.0, False, "verified", "Gio"),  # huge amount but domestic
    ])
    watchlist = make_watchlist_df(spark, [])

    result, _ = gold.apply_fraud_flags(txns, watchlist, test_config)
    row = result.filter(result.txn_id == "TXN-21").collect()[0]

    assert row["is_high_value_international"] is False


def test_risky_kyc_status_flags_suspicious(spark, test_config):
    txns = make_txn_df(spark, [
        ("ACC-030", "TXN-30", 100.0, False, "pending", "Hina"),
        ("ACC-031", "TXN-31", 100.0, False, "verified", "Ian"),
    ])
    watchlist = make_watchlist_df(spark, [])

    result, _ = gold.apply_fraud_flags(txns, watchlist, test_config)
    rows = {row["txn_id"]: row for row in result.collect()}

    assert rows["TXN-30"]["is_pending_kyc_risk"] is True
    assert rows["TXN-30"]["fraud_risk_level"] == "suspicious"
    assert rows["TXN-31"]["fraud_risk_level"] == "normal"


def test_normal_when_no_signals_triggered(spark, test_config):
    txns = make_txn_df(spark, [
        ("ACC-040", "TXN-40", 50.0, False, "verified", "Jai"),
    ])
    watchlist = make_watchlist_df(spark, [])

    result, _ = gold.apply_fraud_flags(txns, watchlist, test_config)
    row = result.collect()[0]

    assert row["is_watchlisted"] is False
    assert row["is_high_value_international"] is False
    assert row["is_pending_kyc_risk"] is False
    assert row["fraud_risk_level"] == "normal"


def test_fraud_summary_counts_add_up(spark, test_config):
    txns = spark.createDataFrame([
        ("ACC-050", "TXN-50", 100.0, False, "verified", "Kim", "Mumbai_Main", "savings"),
        ("ACC-051", "TXN-51", 100.0, False, "pending", "Lee", "Mumbai_Main", "savings"),
    ], ["account_id", "txn_id", "amount", "is_international", "kyc_status", "customer_name", "branch", "account_type"])
    watchlist = make_watchlist_df(spark, [])

    result, _ = gold.apply_fraud_flags(txns, watchlist, test_config)
    summary = gold.build_fraud_summary(result)
    row = summary.filter(summary.branch == "Mumbai_Main").collect()[0]

    assert row["total_transactions"] == 2
    assert row["suspicious_count"] == 1
    assert row["confirmed_fraud_count"] == 0
    assert row["fraud_rate_pct"] == 50.0


def test_account_breakdown_excludes_normal_transactions(spark, test_config):
    txns = make_txn_df(spark, [
        ("ACC-060", "TXN-60", 100.0, False, "verified", "Mona"),   # normal
        ("ACC-061", "TXN-61", 100.0, False, "pending", "Nina"),    # suspicious
    ])
    watchlist = make_watchlist_df(spark, [])

    result, _ = gold.apply_fraud_flags(txns, watchlist, test_config)
    breakdown = gold.build_account_level_breakdown(result)
    account_ids = [row["account_id"] for row in breakdown.collect()]

    assert "ACC-060" not in account_ids
    assert "ACC-061" in account_ids
