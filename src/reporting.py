import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def load_config(config_path: str = "config/pipeline_config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def build_fraud_type_breakdown(fraud_df: DataFrame) -> DataFrame:
    """Transaction count, total amount, and distinct accounts per known fraud_type."""
    return (
        fraud_df.filter(F.col("fraud_type").isNotNull())
          .groupBy("fraud_type")
          .agg(
              F.count("*").alias("transaction_count"),
              F.round(F.sum("amount"), 2).alias("total_amount"),
              F.countDistinct("account_id").alias("distinct_accounts"),
          )
          .orderBy(F.col("transaction_count").desc())
    )


def build_monthly_fraud_trend(fraud_df: DataFrame) -> DataFrame:
    """Total / confirmed_fraud / suspicious counts by transaction month."""
    return (
        fraud_df.withColumn("txn_month", F.date_format("txn_date", "yyyy-MM"))
          .groupBy("txn_month")
          .agg(
              F.count("*").alias("total_transactions"),
              F.sum(F.when(F.col("fraud_risk_level") == "confirmed_fraud", 1).otherwise(0)).alias("confirmed_fraud_count"),
              F.sum(F.when(F.col("fraud_risk_level") == "suspicious", 1).otherwise(0)).alias("suspicious_count"),
          )
          .orderBy("txn_month")
    )


def build_city_fraud_breakdown(fraud_df: DataFrame) -> DataFrame:
    """Fraud rate by transaction city."""
    return (
        fraud_df.groupBy("city")
          .agg(
              F.count("*").alias("total_transactions"),
              F.sum(F.when(F.col("fraud_risk_level") == "confirmed_fraud", 1).otherwise(0)).alias("confirmed_fraud_count"),
              F.sum(F.when(F.col("fraud_risk_level") == "suspicious", 1).otherwise(0)).alias("suspicious_count"),
          )
          .withColumn(
              "fraud_rate_pct",
              F.round((F.col("confirmed_fraud_count") + F.col("suspicious_count")) / F.col("total_transactions") * 100, 2)
          )
          .orderBy(F.col("fraud_rate_pct").desc())
    )


def build_top_flagged_accounts(account_breakdown_df: DataFrame, top_n: int = 10) -> DataFrame:
    """Top N accounts by total flagged amount (expects gold.build_account_level_breakdown output)."""
    return account_breakdown_df.orderBy(F.col("total_flagged_amount").desc()).limit(top_n)


def build_top_flagged_merchants(fraud_df: DataFrame, top_n: int = 10) -> DataFrame:
    """Top N merchants by count of flagged (non-normal) transactions."""
    return (
        fraud_df.filter(F.col("fraud_risk_level") != "normal")
          .groupBy("merchant")
          .agg(
              F.count("*").alias("flagged_transaction_count"),
              F.round(F.sum("amount"), 2).alias("total_flagged_amount"),
          )
          .orderBy(F.col("flagged_transaction_count").desc())
          .limit(top_n)
    )


def build_kyc_risk_breakdown(fraud_df: DataFrame) -> DataFrame:
    """Transaction and fraud counts by account KYC status."""
    return (
        fraud_df.groupBy("kyc_status")
          .agg(
              F.count("*").alias("total_transactions"),
              F.sum(F.when(F.col("fraud_risk_level") == "confirmed_fraud", 1).otherwise(0)).alias("confirmed_fraud_count"),
              F.sum(F.when(F.col("fraud_risk_level") == "suspicious", 1).otherwise(0)).alias("suspicious_count"),
          )
          .withColumn(
              "fraud_rate_pct",
              F.round((F.col("confirmed_fraud_count") + F.col("suspicious_count")) / F.col("total_transactions") * 100, 2)
          )
          .orderBy(F.col("fraud_rate_pct").desc())
    )


def build_international_domestic_split(fraud_df: DataFrame) -> DataFrame:
    """Transaction count and fraud rate comparison between international and domestic transactions."""
    return (
        fraud_df.groupBy("is_international")
          .agg(
              F.count("*").alias("total_transactions"),
              F.sum(F.when(F.col("fraud_risk_level") == "confirmed_fraud", 1).otherwise(0)).alias("confirmed_fraud_count"),
              F.sum(F.when(F.col("fraud_risk_level") == "suspicious", 1).otherwise(0)).alias("suspicious_count"),
          )
          .withColumn(
              "fraud_rate_pct",
              F.round((F.col("confirmed_fraud_count") + F.col("suspicious_count")) / F.col("total_transactions") * 100, 2)
          )
    )


def build_risk_level_counts(fraud_df: DataFrame) -> DataFrame:
    """Transaction count and share of total per fraud_risk_level (normal/suspicious/confirmed_fraud).

    This was previously only ever printed to console (gold.run_gold_processing's
    risk_level_counts) or shown via .display() in the notebook — never persisted.
    """
    total = fraud_df.count()
    return (
        fraud_df.groupBy("fraud_risk_level")
          .agg(F.count("*").alias("transaction_count"))
          .withColumn("pct_of_total", F.round(F.col("transaction_count") / F.lit(total) * 100, 2))
          .orderBy(F.col("transaction_count").desc())
    )


def generate_overall_summary_report(fraud_df: DataFrame, threshold: float, config: dict) -> str:
    """Markdown summary of total transactions and risk-level breakdown for the whole Gold dataset."""
    total = fraud_df.count()
    counts = {row["fraud_risk_level"]: row["count"] for row in fraud_df.groupBy("fraud_risk_level").count().collect()}

    confirmed = counts.get("confirmed_fraud", 0)
    suspicious = counts.get("suspicious", 0)
    normal = counts.get("normal", 0)

    def pct(part: int) -> float:
        return round(part / total * 100, 2) if total else 0.0

    lines = [
        "# Overall Fraud Summary",
        "",
        "Generated automatically by the Gold layer. Totals across all processed transactions.",
        "",
        "## Totals",
        f"- Total Transactions: {total}",
        f"- Confirmed Fraud: {confirmed} ({pct(confirmed)}%)",
        f"- Suspicious: {suspicious} ({pct(suspicious)}%)",
        f"- Normal: {normal} ({pct(normal)}%)",
        "",
        "## Thresholds Used",
        f"- High-value percentile: {config['gold']['high_value_percentile']}",
        f"- High-value amount threshold: {round(threshold, 2)}",
        f"- Risky KYC statuses: {', '.join(config['gold']['risky_kyc_statuses'])}",
        "",
    ]

    report_text = "\n".join(lines)

    output_path = config["output"]["overall_summary_path"]
    with open(output_path, "w") as f:
        f.write(report_text)

    return report_text


def export_all_reports(fraud_df: DataFrame, account_breakdown_df: DataFrame, threshold: float, config: dict) -> dict:
    """Builds every Gold-layer insight report and writes it to outputs/.

    Includes reports that were already being shown in the notebook via .display()
    (full account_breakdown_df, risk level counts) but were never actually written
    to outputs/ as their own files — only the derived top-10 accounts and console
    prints existed before.
    """
    top_n = config["output"].get("top_n_flagged", 10)

    reports = {
        "fraud_type_breakdown": build_fraud_type_breakdown(fraud_df),
        "monthly_fraud_trend": build_monthly_fraud_trend(fraud_df),
        "city_fraud_breakdown": build_city_fraud_breakdown(fraud_df),
        "top_flagged_accounts": build_top_flagged_accounts(account_breakdown_df, top_n),
        "top_flagged_merchants": build_top_flagged_merchants(fraud_df, top_n),
        "kyc_risk_breakdown": build_kyc_risk_breakdown(fraud_df),
        "international_vs_domestic": build_international_domestic_split(fraud_df),
        "account_breakdown_full": account_breakdown_df,
        "risk_level_counts": build_risk_level_counts(fraud_df),
    }

    path_keys = {
        "fraud_type_breakdown": "fraud_type_breakdown_path",
        "monthly_fraud_trend": "monthly_fraud_trend_path",
        "city_fraud_breakdown": "city_fraud_breakdown_path",
        "top_flagged_accounts": "top_flagged_accounts_path",
        "top_flagged_merchants": "top_flagged_merchants_path",
        "kyc_risk_breakdown": "kyc_risk_breakdown_path",
        "international_vs_domestic": "international_domestic_split_path",
        "account_breakdown_full": "account_breakdown_full_path",
        "risk_level_counts": "risk_level_counts_path",
    }

    written_paths = {}
    for name, df in reports.items():
        output_path = config["output"][path_keys[name]]
        df.toPandas().to_csv(output_path, index=False)
        written_paths[name] = output_path
        print(f"[reporting] {name} -> {output_path}")

    summary_text = generate_overall_summary_report(fraud_df, threshold, config)
    written_paths["overall_summary"] = config["output"]["overall_summary_path"]
    print(f"[reporting] overall_summary -> {written_paths['overall_summary']}")

    return written_paths
