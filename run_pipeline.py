"""
run_pipeline.py
================
Single entrypoint that runs Bronze -> Silver -> Gold end to end, then
generates the data quality report and exports sample CSVs to outputs/.

Meant for running this project outside Databricks (e.g. locally, or in CI).
On Databricks, use the notebooks in notebooks/ instead — they reuse the
cluster's existing `spark` session rather than creating a new local one.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from pyspark.sql import SparkSession

import bronze
import silver
import gold
import data_quality


def get_local_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("SmartFraudDetectionPipeline")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    try:
        from delta import configure_spark_with_delta_pip
        return configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        raise RuntimeError(
            "delta-spark is not installed. Run: pip install delta-spark"
        )


def export_gold_outputs(spark, gold_result, config):
    fraud_df = spark.read.format("delta").load(gold_result["fraud_transactions_path"])
    summary_df = spark.read.format("delta").load(gold_result["fraud_summary_path"])

    sample_path = config["output"]["sample_export_path"]
    summary_path = config["output"]["summary_export_path"]

    fraud_df.toPandas().to_csv(sample_path, index=False)
    summary_df.toPandas().to_csv(summary_path, index=False)

    print(f"[export] gold sample -> {sample_path}")
    print(f"[export] fraud summary -> {summary_path}")


def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    os.makedirs("outputs", exist_ok=True)

    spark = get_local_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print("=== BRONZE ===")
    bronze.run_bronze_ingestion(spark)

    print("\n=== SILVER ===")
    silver.run_silver_processing(spark)

    print("\n=== GOLD ===")
    gold_result = gold.run_gold_processing(spark)

    print("\n=== DATA QUALITY REPORT ===")
    config = bronze.load_config()
    bronze_accounts = bronze.read_source_csv(spark, "accounts", config)
    bronze_transactions = bronze.read_source_csv(spark, "transactions", config)
    silver_enriched = spark.read.format("delta").load(
        f"{config['tables']['silver']['path_prefix']}/{config['tables']['silver']['enriched_transactions']}"
    )
    report = data_quality.generate_data_quality_report(
        spark, bronze_accounts, bronze_transactions, silver_enriched, config
    )
    print(report)

    print("\n=== EXPORTING OUTPUTS ===")
    export_gold_outputs(spark, gold_result, config)

    print("\nPipeline finished successfully.")


if __name__ == "__main__":
    main()
