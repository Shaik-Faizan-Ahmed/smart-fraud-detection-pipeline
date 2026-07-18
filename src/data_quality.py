import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def load_config(config_path: str = "config/pipeline_config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def generate_data_quality_report(
    spark: SparkSession,
    bronze_accounts: DataFrame,
    bronze_transactions: DataFrame,
    silver_enriched: DataFrame,
    config: dict,
) -> str:
    """Builds a markdown summary of what the pipeline actually found/fixed."""

    total_accounts = bronze_accounts.count()
    total_transactions = bronze_transactions.count()

    null_credit_limit = bronze_accounts.filter(F.col("credit_limit").isNull()).count()
    null_merchant = bronze_transactions.filter(F.col("merchant").isNull()).count()

    distinct_txn_ids = bronze_transactions.select("txn_id").distinct().count()
    duplicate_txn_ids_removed = total_transactions - distinct_txn_ids

    silver_row_count = silver_enriched.count()
    orphan_count = silver_enriched.filter(F.col("is_orphan_transaction") == True).count()

    lines = [
        "# Data Quality Report",
        "",
        "Generated automatically by the pipeline. Summarizes what Bronze/Silver found and fixed.",
        "",
        "## Source Row Counts",
        f"- Accounts: {total_accounts}",
        f"- Transactions: {total_transactions}",
        "",
        "## Nulls Found in Bronze (before cleaning)",
        f"- Missing `credit_limit`: {null_credit_limit} accounts",
        f"- Missing `merchant`: {null_merchant} transactions",
        "",
        "## Duplicates",
        f"- Duplicate `txn_id` rows removed: {duplicate_txn_ids_removed}",
        "",
        "## Silver Layer Result",
        f"- Enriched transaction rows: {silver_row_count}",
        f"- Orphan transactions (account_id not found in accounts.csv): {orphan_count}",
        "",
    ]

    report_text = "\n".join(lines)

    output_path = config["output"]["data_quality_report_path"]
    with open(output_path, "w") as f:
        f.write(report_text)

    return report_text
