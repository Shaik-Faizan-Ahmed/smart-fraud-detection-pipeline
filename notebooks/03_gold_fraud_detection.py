# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Layer — Fraud Detection & Insights
# MAGIC Applies fraud watchlist matching plus two extra risk signals
# MAGIC (high-value international, risky KYC status), assigns a combined
# MAGIC risk level, and builds summary tables.

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

import gold

result = gold.run_gold_processing(spark)
result

# COMMAND ----------

# MAGIC %md
# MAGIC ### Risk level breakdown
# MAGIC - `confirmed_fraud` — account is on the known fraud watchlist
# MAGIC - `suspicious` — high-value international transaction, or account has risky KYC status
# MAGIC - `normal` — no risk signals triggered

# COMMAND ----------

fraud_df = spark.read.format("delta").load(result["fraud_transactions_path"])
fraud_df.groupBy("fraud_risk_level").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fraud rate by branch and account type

# COMMAND ----------

summary_df = spark.read.format("delta").load(result["fraud_summary_path"])
summary_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top flagged accounts by total flagged amount

# COMMAND ----------

breakdown_df = gold.build_account_level_breakdown(fraud_df)
breakdown_df.display()
