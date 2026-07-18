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

# MAGIC %md
# MAGIC ### Run Gold Processing
# MAGIC Joins Silver transactions against the fraud watchlist, computes the
# MAGIC high-value threshold, assigns `fraud_risk_level` to every transaction,
# MAGIC and writes the classified transactions + summary table as Delta
# MAGIC tables. Output below shows the threshold used and the paths written.

# COMMAND ----------

import gold
import reporting

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Additional Gold layer insights
# MAGIC Fraud type breakdown, monthly trend, city/KYC/international views, and
# MAGIC top flagged accounts + merchants — written to `outputs/` alongside an
# MAGIC overall summary report. The full account breakdown and risk level
# MAGIC counts below were already shown via .display() previously but are now
# MAGIC also persisted as their own files (account_breakdown_full.csv,
# MAGIC risk_level_counts.csv) instead of only appearing in this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fraud Type Breakdown
# MAGIC Transaction count, total dollar amount, and number of distinct accounts
# MAGIC for each known fraud type on the watchlist. Shows which fraud type
# MAGIC carries the largest dollar exposure.

# COMMAND ----------

reporting.build_fraud_type_breakdown(fraud_df).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Monthly Fraud Trend
# MAGIC Total, confirmed-fraud, and suspicious transaction counts grouped by
# MAGIC month, so any spike or drift in fraud activity over time is visible.

# COMMAND ----------

reporting.build_monthly_fraud_trend(fraud_df).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fraud Rate by City
# MAGIC Same total/confirmed/suspicious breakdown as above, grouped by
# MAGIC transaction city, with a computed `fraud_rate_pct`, sorted
# MAGIC highest-risk city first.

# COMMAND ----------

reporting.build_city_fraud_breakdown(fraud_df).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fraud Rate by KYC Status
# MAGIC Tests whether risky KYC status (pending/rejected/expired) actually
# MAGIC correlates with higher fraud rates, broken out by `kyc_status`.

# COMMAND ----------

reporting.build_kyc_risk_breakdown(fraud_df).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### International vs. Domestic Fraud Rate
# MAGIC Direct comparison of fraud rate between international and domestic
# MAGIC transactions.

# COMMAND ----------

reporting.build_international_domestic_split(fraud_df).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Top Flagged Accounts
# MAGIC Highest-risk accounts ranked by total flagged transaction amount
# MAGIC (top N, configurable via `output.top_n_flagged`).

# COMMAND ----------

reporting.build_top_flagged_accounts(breakdown_df).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Top Flagged Merchants
# MAGIC Merchants most frequently tied to flagged (non-normal) transactions,
# MAGIC ranked by flagged transaction count.

# COMMAND ----------

reporting.build_top_flagged_merchants(fraud_df).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export all reports to outputs/
# MAGIC Writes every table above to its own CSV in `outputs/`, plus the
# MAGIC markdown overall summary report — the persisted version of
# MAGIC everything just displayed inline in this notebook.

# COMMAND ----------

reporting.export_all_reports(fraud_df, breakdown_df, result["threshold"], gold.load_config())
