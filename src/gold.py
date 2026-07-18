import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def load_config(config_path: str = "config/pipeline_config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def read_silver_table(spark: SparkSession, config: dict) -> DataFrame:
    env = config["environment"]
    table_name = config["tables"]["silver"]["enriched_transactions"]
    path_prefix = config["tables"]["silver"]["path_prefix"][env]
    path = f"{path_prefix}/{table_name}"
    return spark.read.format("delta").load(path)


def read_bronze_fraud_watchlist(spark: SparkSession, config: dict) -> DataFrame:
    env = config["environment"]
    table_name = config["tables"]["bronze"]["fraud_watchlist"]
    path_prefix = config["tables"]["bronze"]["path_prefix"][env]
    path = f"{path_prefix}/{table_name}"
    return spark.read.format("delta").load(path)


def apply_fraud_flags(txn_df: DataFrame, fraud_watchlist_df: DataFrame, config: dict) -> DataFrame:
    """Adds is_watchlisted, is_high_value_international, is_pending_kyc_risk, and fraud_risk_level."""

    # confirmed fraud: account appears in the watchlist
    watchlist_accounts = fraud_watchlist_df.select("account_id", "fraud_type").distinct()
    df = txn_df.join(watchlist_accounts, on="account_id", how="left")
    df = df.withColumn("is_watchlisted", F.col("fraud_type").isNotNull())

    # high value + international: amount above the configured percentile, and international
    percentile = config["gold"]["high_value_percentile"]
    threshold = df.approxQuantile("amount", [percentile], 0.01)[0]
    df = df.withColumn(
        "is_high_value_international",
        (F.col("is_international") == True) & (F.col("amount") >= F.lit(threshold))
    )

    # pending/rejected/expired KYC on the account
    risky_statuses = config["gold"]["risky_kyc_statuses"]
    df = df.withColumn("is_pending_kyc_risk", F.col("kyc_status").isin(risky_statuses))

    # combined risk level
    df = df.withColumn(
        "fraud_risk_level",
        F.when(F.col("is_watchlisted"), F.lit("confirmed_fraud"))
         .when(F.col("is_high_value_international") | F.col("is_pending_kyc_risk"), F.lit("suspicious"))
         .otherwise(F.lit("normal"))
    )

    return df, threshold


def build_fraud_summary(df: DataFrame) -> DataFrame:
    """Aggregated fraud rate by branch, account_type, city, and merchant."""
    return (
        df.groupBy("branch", "account_type")
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


def build_account_level_breakdown(df: DataFrame) -> DataFrame:
    """Per-account fraud transaction counts, for accounts with at least one flagged transaction."""
    return (
        df.filter(F.col("fraud_risk_level") != "normal")
          .groupBy("account_id", "customer_name", "fraud_risk_level")
          .agg(F.count("*").alias("flagged_transaction_count"), F.sum("amount").alias("total_flagged_amount"))
          .orderBy(F.col("total_flagged_amount").desc())
    )


def write_gold_tables(fraud_df: DataFrame, summary_df: DataFrame, config: dict) -> dict:
    env = config["environment"]
    path_prefix = config["tables"]["gold"]["path_prefix"][env]

    fraud_path = f"{path_prefix}/{config['tables']['gold']['fraud_transactions']}"
    summary_path = f"{path_prefix}/{config['tables']['gold']['fraud_summary']}"

    fraud_df.write.format("delta").mode("overwrite").save(fraud_path)
    summary_df.write.format("delta").mode("overwrite").save(summary_path)

    return {"fraud_transactions_path": fraud_path, "fraud_summary_path": summary_path}


def run_gold_processing(spark: SparkSession, config_path: str = "config/pipeline_config.yaml") -> dict:
    config = load_config(config_path)

    enriched = read_silver_table(spark, config)
    watchlist = read_bronze_fraud_watchlist(spark, config)

    fraud_df, threshold = apply_fraud_flags(enriched, watchlist, config)
    summary_df = build_fraud_summary(fraud_df)

    paths = write_gold_tables(fraud_df, summary_df, config)

    counts = fraud_df.groupBy("fraud_risk_level").count().collect()
    counts_dict = {row["fraud_risk_level"]: row["count"] for row in counts}

    print(f"[gold] high-value threshold (amount >= {round(threshold, 2)})")
    print(f"[gold] risk level counts: {counts_dict}")
    print(f"[gold] fraud_transactions -> {paths['fraud_transactions_path']}")
    print(f"[gold] fraud_summary -> {paths['fraud_summary_path']}")

    return {
        "threshold": threshold,
        "risk_level_counts": counts_dict,
        **paths,
    }


if __name__ == "__main__":
    spark = SparkSession.builder.appName("SmartFraudDetection_Gold").getOrCreate()
    run_gold_processing(spark)
