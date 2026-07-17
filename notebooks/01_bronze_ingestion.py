# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Layer — Raw Data Ingestion
# MAGIC Reads the 3 source CSVs with enforced schemas, tags them with ingestion
# MAGIC metadata, and writes them out as Delta tables.

# COMMAND ----------

import sys
import os

notebook_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
repo_root = os.path.abspath(os.path.join(notebook_dir, ".."))
src_path = os.path.join(repo_root, "src")
if src_path not in sys.path:
    sys.path.append(src_path)

os.chdir(repo_root)  # so relative paths in pipeline_config.yaml resolve correctly

# COMMAND ----------

import bronze

results = bronze.run_bronze_ingestion(spark)
results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick sanity check
# MAGIC Row counts should be 50 accounts, 200 transactions, 10 fraud watchlist records.

# COMMAND ----------

for name, info in results.items():
    print(f"{name}: {info['row_count']} rows -> {info['output_path']}")

# COMMAND ----------

spark.read.format("delta").load(results["transactions"]["output_path"]).display()
