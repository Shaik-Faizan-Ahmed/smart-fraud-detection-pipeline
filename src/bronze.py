import os
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from schemas import SCHEMA_REGISTRY


def load_config(config_path: str = "config/pipeline_config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def read_source_csv(spark: SparkSession, dataset_name: str, config: dict) -> DataFrame:
    """Reads a raw CSV with its enforced schema and tags it with bronze metadata."""
    path_key = {
        "accounts": "accounts_path",
        "transactions": "transactions_path",
        "fraud_watchlist": "fraud_watchlist_path",
    }[dataset_name]

    source_path = config["source"][path_key]
    schema = SCHEMA_REGISTRY[dataset_name]

    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(source_path)
    )

    df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
           .withColumn("source_file", F.lit(os.path.basename(source_path)))

    return df


def write_bronze_table(df: DataFrame, dataset_name: str, config: dict) -> str:
    table_key = {
        "accounts": "accounts",
        "transactions": "transactions",
        "fraud_watchlist": "fraud_watchlist",
    }[dataset_name]

    table_name = config["tables"]["bronze"][table_key]
    path_prefix = config["tables"]["bronze"]["path_prefix"]
    output_path = f"{path_prefix}/{table_name}"

    df.write.format("delta").mode("overwrite").save(output_path)

    return output_path


def run_bronze_ingestion(spark: SparkSession, config_path: str = "config/pipeline_config.yaml") -> dict:
    """Runs bronze ingestion for all 3 datasets using an existing SparkSession."""
    config = load_config(config_path)

    results = {}
    for dataset_name in ["accounts", "transactions", "fraud_watchlist"]:
        df = read_source_csv(spark, dataset_name, config)
        output_path = write_bronze_table(df, dataset_name, config)
        results[dataset_name] = {
            "row_count": df.count(),
            "output_path": output_path,
        }
        print(f"[bronze] {dataset_name}: {results[dataset_name]['row_count']} rows -> {output_path}")

    return results


if __name__ == "__main__":
    # standalone/local run (not needed on Databricks, spark is already provided there)
    spark = SparkSession.builder.appName("SmartFraudDetection_Bronze").getOrCreate()
    run_bronze_ingestion(spark)
