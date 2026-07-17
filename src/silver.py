import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def load_config(config_path: str = "config/pipeline_config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def read_bronze_table(spark: SparkSession, dataset_name: str, config: dict) -> DataFrame:
    table_key = {
        "accounts": "accounts",
        "transactions": "transactions",
        "fraud_watchlist": "fraud_watchlist",
    }[dataset_name]

    table_name = config["tables"]["bronze"][table_key]
    path_prefix = config["tables"]["bronze"]["path_prefix"]
    path = f"{path_prefix}/{table_name}"

    return spark.read.format("delta").load(path)


def clean_accounts(df: DataFrame) -> DataFrame:
    """Casts dates, imputes missing credit_limit with the median for that account_type."""
    df = df.withColumn("opening_date", F.to_date("opening_date", "yyyy-MM-dd"))

    median_by_type = df.groupBy("account_type").agg(
        F.expr("percentile_approx(credit_limit, 0.5)").alias("median_credit_limit")
    )

    df = df.join(median_by_type, on="account_type", how="left")
    df = df.withColumn(
        "credit_limit",
        F.when(F.col("credit_limit").isNull(), F.col("median_credit_limit")).otherwise(F.col("credit_limit"))
    ).drop("median_credit_limit")

    return df


def clean_transactions(df: DataFrame, config: dict) -> DataFrame:
    """Casts dates/types, fills missing merchant, dedupes on txn_id."""
    merchant_fill = config["silver"]["null_handling"]["merchant"]

    df = df.withColumn("txn_date", F.to_date("txn_date", "yyyy-MM-dd"))
    df = df.withColumn("is_international", F.when(F.col("is_international") == "yes", True).otherwise(False))
    df = df.withColumn(
        "merchant",
        F.when(F.col("merchant").isNull(), F.lit(merchant_fill)).otherwise(F.col("merchant"))
    )

    if config["silver"]["drop_duplicate_txn_ids"]:
        window = Window.partitionBy("txn_id").orderBy(F.col("ingestion_timestamp").desc())
        df = df.withColumn("_row_num", F.row_number().over(window)) \
               .filter(F.col("_row_num") == 1) \
               .drop("_row_num")

    return df


def enrich_transactions(txn_df: DataFrame, acc_df: DataFrame, config: dict) -> DataFrame:
    """Left-joins transactions to accounts, flagging transactions whose account_id has no match."""
    acc_cols = acc_df.select(
        "account_id", "customer_name", "account_type", "branch", "kyc_status", "credit_limit"
    )

    joined = txn_df.join(acc_cols, on="account_id", how="left")

    if config["silver"]["flag_orphan_transactions"]:
        joined = joined.withColumn("is_orphan_transaction", F.col("customer_name").isNull())

    return joined


def write_silver_table(df: DataFrame, config: dict) -> str:
    table_name = config["tables"]["silver"]["enriched_transactions"]
    path_prefix = config["tables"]["silver"]["path_prefix"]
    output_path = f"{path_prefix}/{table_name}"

    df.write.format("delta").mode("overwrite").save(output_path)

    return output_path


def run_silver_processing(spark: SparkSession, config_path: str = "config/pipeline_config.yaml") -> dict:
    config = load_config(config_path)

    accounts = read_bronze_table(spark, "accounts", config)
    transactions = read_bronze_table(spark, "transactions", config)

    accounts_clean = clean_accounts(accounts)
    transactions_clean = clean_transactions(transactions, config)

    enriched = enrich_transactions(transactions_clean, accounts_clean, config)
    output_path = write_silver_table(enriched, config)

    orphan_count = enriched.filter(F.col("is_orphan_transaction") == True).count()
    total_count = enriched.count()

    print(f"[silver] enriched transactions: {total_count} rows -> {output_path}")
    print(f"[silver] orphan transactions flagged: {orphan_count}")

    return {
        "row_count": total_count,
        "orphan_count": orphan_count,
        "output_path": output_path,
    }


if __name__ == "__main__":
    spark = SparkSession.builder.appName("SmartFraudDetection_Silver").getOrCreate()
    run_silver_processing(spark)
