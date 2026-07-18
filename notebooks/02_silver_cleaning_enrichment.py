# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Layer — Cleaning & Enrichment
# MAGIC Cleans accounts + transactions, casts types, joins on account_id,
# MAGIC and flags transactions whose account_id has no matching account.

# COMMAND ----------

import sys
import os

notebook_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
repo_root = os.path.abspath(os.path.join(notebook_dir, ".."))
src_path = os.path.join(repo_root, "src")
if src_path not in sys.path:
    sys.path.append(src_path)

os.chdir(repo_root)

# COMMAND ----------

import silver
import data_quality

result = silver.run_silver_processing(spark)
result

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanity checks
# MAGIC - Total row count should still be 200 (no rows silently dropped)
# MAGIC - No remaining nulls in `merchant` / `credit_limit`
# MAGIC - Orphan transactions should be flagged, not removed

# COMMAND ----------

enriched = spark.read.format("delta").load(result["output_path"])

print("total rows:", enriched.count())
print("null merchant:", enriched.filter(enriched.merchant.isNull()).count())
print("null credit_limit:", enriched.filter(enriched.credit_limit.isNull()).count())
print("orphan transactions:", enriched.filter(enriched.is_orphan_transaction == True).count())

enriched.filter(enriched.is_orphan_transaction == True).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data quality report
# MAGIC Summarizes what Bronze/Silver found and fixed (nulls, duplicates removed,
# MAGIC orphan transactions). Written to `outputs/data_quality_report.md`.
# MAGIC Previously this only ran via `run_pipeline.py` locally — now it also runs here.

# COMMAND ----------

config = silver.load_config()
bronze_accounts = silver.read_bronze_table(spark, "accounts", config)
bronze_transactions = silver.read_bronze_table(spark, "transactions", config)

report = data_quality.generate_data_quality_report(
    spark, bronze_accounts, bronze_transactions, enriched, config
)
print(report)
