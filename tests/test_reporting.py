import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, DateType
)
import datetime

import reporting

FRAUD_SCHEMA = StructType([
    StructField("account_id", StringType()),
    StructField("txn_id", StringType()),
    StructField("txn_date", DateType()),
    StructField("amount", DoubleType()),
    StructField("merchant", StringType()),
    StructField("city", StringType()),
    StructField("is_international", BooleanType()),
    StructField("kyc_status", StringType()),
    StructField("customer_name", StringType()),
    StructField("fraud_type", StringType()),
    StructField("fraud_risk_level", StringType()),
])


def make_fraud_df(spark, rows):
    return spark.createDataFrame(rows, FRAUD_SCHEMA)


def test_fraud_type_breakdown_excludes_unwatchlisted(spark):
    df = make_fraud_df(spark, [
        ("ACC-1", "T1", datetime.date(2026, 1, 5), 100.0, "M1", "Mumbai", False, "verified", "A", "card_cloning", "confirmed_fraud"),
        ("ACC-2", "T2", datetime.date(2026, 1, 6), 200.0, "M2", "Delhi", False, "verified", "B", None, "normal"),
    ])

    result = reporting.build_fraud_type_breakdown(df).collect()

    assert len(result) == 1
    assert result[0]["fraud_type"] == "card_cloning"
    assert result[0]["transaction_count"] == 1


def test_monthly_fraud_trend_groups_by_month(spark):
    df = make_fraud_df(spark, [
        ("ACC-1", "T1", datetime.date(2026, 1, 5), 100.0, "M1", "Mumbai", False, "verified", "A", None, "normal"),
        ("ACC-2", "T2", datetime.date(2026, 1, 20), 200.0, "M2", "Delhi", False, "verified", "B", "money_laundering", "confirmed_fraud"),
        ("ACC-3", "T3", datetime.date(2026, 2, 1), 300.0, "M3", "Pune", False, "verified", "C", None, "normal"),
    ])

    result = {row["txn_month"]: row for row in reporting.build_monthly_fraud_trend(df).collect()}

    assert result["2026-01"]["total_transactions"] == 2
    assert result["2026-01"]["confirmed_fraud_count"] == 1
    assert result["2026-02"]["total_transactions"] == 1


def test_city_fraud_breakdown_computes_rate(spark):
    df = make_fraud_df(spark, [
        ("ACC-1", "T1", datetime.date(2026, 1, 5), 100.0, "M1", "Mumbai", False, "verified", "A", "card_cloning", "confirmed_fraud"),
        ("ACC-2", "T2", datetime.date(2026, 1, 6), 200.0, "M2", "Mumbai", False, "verified", "B", None, "normal"),
    ])

    row = reporting.build_city_fraud_breakdown(df).filter("city = 'Mumbai'").collect()[0]

    assert row["total_transactions"] == 2
    assert row["confirmed_fraud_count"] == 1
    assert row["fraud_rate_pct"] == 50.0


def test_top_flagged_merchants_excludes_normal(spark):
    df = make_fraud_df(spark, [
        ("ACC-1", "T1", datetime.date(2026, 1, 5), 100.0, "RiskyMerchant", "Mumbai", False, "verified", "A", "card_cloning", "confirmed_fraud"),
        ("ACC-2", "T2", datetime.date(2026, 1, 6), 200.0, "SafeMerchant", "Delhi", False, "verified", "B", None, "normal"),
    ])

    result = reporting.build_top_flagged_merchants(df, top_n=5).collect()
    merchants = [row["merchant"] for row in result]

    assert "RiskyMerchant" in merchants
    assert "SafeMerchant" not in merchants


def test_kyc_risk_breakdown_computes_rate(spark):
    df = make_fraud_df(spark, [
        ("ACC-1", "T1", datetime.date(2026, 1, 5), 100.0, "M1", "Mumbai", False, "pending", "A", None, "suspicious"),
        ("ACC-2", "T2", datetime.date(2026, 1, 6), 200.0, "M2", "Delhi", False, "pending", "B", None, "normal"),
    ])

    row = reporting.build_kyc_risk_breakdown(df).filter("kyc_status = 'pending'").collect()[0]

    assert row["total_transactions"] == 2
    assert row["suspicious_count"] == 1
    assert row["fraud_rate_pct"] == 50.0


def test_international_domestic_split(spark):
    df = make_fraud_df(spark, [
        ("ACC-1", "T1", datetime.date(2026, 1, 5), 100.0, "M1", "Mumbai", True, "verified", "A", None, "normal"),
        ("ACC-2", "T2", datetime.date(2026, 1, 6), 200.0, "M2", "Delhi", False, "verified", "B", None, "normal"),
    ])

    result = {row["is_international"]: row for row in reporting.build_international_domestic_split(df).collect()}

    assert result[True]["total_transactions"] == 1
    assert result[False]["total_transactions"] == 1


def test_risk_level_counts_sum_to_total(spark):
    df = make_fraud_df(spark, [
        ("ACC-1", "T1", datetime.date(2026, 1, 5), 100.0, "M1", "Mumbai", False, "verified", "A", "card_cloning", "confirmed_fraud"),
        ("ACC-2", "T2", datetime.date(2026, 1, 6), 200.0, "M2", "Delhi", False, "pending", "B", None, "suspicious"),
        ("ACC-3", "T3", datetime.date(2026, 1, 7), 300.0, "M3", "Pune", False, "verified", "C", None, "normal"),
    ])

    result = {row["fraud_risk_level"]: row for row in reporting.build_risk_level_counts(df).collect()}

    assert result["confirmed_fraud"]["transaction_count"] == 1
    assert result["normal"]["pct_of_total"] == round(1 / 3 * 100, 2)
    assert sum(r["transaction_count"] for r in result.values()) == 3


def test_overall_summary_report_contains_totals(spark, tmp_path):
    df = make_fraud_df(spark, [
        ("ACC-1", "T1", datetime.date(2026, 1, 5), 100.0, "M1", "Mumbai", False, "verified", "A", "card_cloning", "confirmed_fraud"),
        ("ACC-2", "T2", datetime.date(2026, 1, 6), 200.0, "M2", "Delhi", False, "verified", "B", None, "normal"),
    ])

    output_path = str(tmp_path / "overall_summary.md")
    config = {
        "gold": {
            "high_value_percentile": 0.90,
            "risky_kyc_statuses": ["pending", "rejected", "expired"],
        },
        "output": {"overall_summary_path": output_path},
    }

    text = reporting.generate_overall_summary_report(df, threshold=5000.0, config=config)

    assert "Total Transactions: 2" in text
    assert "Confirmed Fraud: 1" in text
    assert os.path.exists(output_path)
