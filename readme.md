# Smart Fraud Detection Pipeline

A scalable data engineering pipeline built with **PySpark**, **Spark SQL**, and **Delta Lake** on **Databricks**, designed to detect fraudulent financial transactions using the **Medallion Architecture** (Bronze → Silver → Gold).

This project was built as part of the **Celebal Excellence Internship (CEI)** — Data Engineering track.

### Student Details

| Field | Detail |
|---|---|
| Name | Shaik Faizan Ahmed |
| Student ID | CT_CSI_DE_641 |
| College Roll Number | 23B81A05L3 |
| College | CVR College of Engineering |

---

## Problem Statement

Financial institutions process large volumes of transactions daily, making it difficult to manually detect fraudulent activity. This project builds a data pipeline that ingests account, transaction, and fraud-watchlist data, cleans and enriches it, and applies fraud-detection logic to classify transactions and surface actionable insights.

> **Note:** the base problem statement asks for a binary `fraud` / `normal` classification driven by watchlist matching. This project intentionally goes a step further by also flagging `suspicious` transactions — those that aren't yet confirmed on the watchlist but carry independent risk signals (unusually large international transfers, or an account with an unresolved/risky KYC status). This surfaces accounts worth a human review before they're ever confirmed as fraud, which is closer to how real fraud detection systems triage transactions in practice. See **Gold Layer Risk Logic** below for the exact rules.

## Architecture

```
Source (CSV Files)
      ↓
Bronze Layer   — Raw ingestion into Delta tables (schema-enforced, traceable)
      ↓
Silver Layer   — Cleaning, type casting, null handling, join accounts + transactions
      ↓
Gold Layer     — Fraud detection logic, risk scoring, aggregated business insights
```

## How to Run

### Locally

```powershell
cd smart-fraud-detection-pipeline
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Set `environment: local` in `config/pipeline_config.yaml`, then run the full pipeline end to end:

```powershell
python run_pipeline.py
```

This runs Bronze → Silver → Gold, generates the data quality report, and exports all insight reports to `outputs/`.

To run tests:

```powershell
pytest
```

### On Databricks

1. Upload `accounts.csv`, `transactions.csv`, and `known_fraud_accounts.csv` to the configured Volume path.
2. Set `environment: databricks` in `config/pipeline_config.yaml`.
3. Open the repo in Databricks Repos and run the notebooks in order:
   - `notebooks/01_bronze_ingestion.py`
   - `notebooks/02_silver_cleaning_enrichment.py` (also generates the data quality report)
   - `notebooks/03_gold_fraud_detection.py`

Each notebook reuses the cluster's existing `spark` session. The Gold notebook also builds and exports all insight reports (same as `reporting.export_all_reports` in the local run).

## Datasets

| Dataset | Description |
|---|---|
| `accounts.csv` | Account holder details: account_id, customer_name, account_type, opening_date, branch, kyc_status, credit_limit |
| `transactions.csv` | Transaction records: txn_id, account_id, txn_date, txn_type, amount, merchant, city, is_international |
| `known_fraud_accounts.csv` | Fraud watchlist: account_id, fraud_type, flagged_date |

## Gold Layer Risk Logic

Each transaction is classified into one of three risk levels based on three signals:

| Risk Level | Triggered When |
|---|---|
| `confirmed_fraud` | Account appears on the fraud watchlist (`is_watchlisted`) |
| `suspicious` | High-value international transaction (amount ≥ configured percentile threshold) **or** account has a risky KYC status (`pending` / `rejected` / `expired`) |
| `normal` | None of the above signals triggered |

Thresholds and risky KYC statuses are configurable in `config/pipeline_config.yaml` under the `gold:` section.

## Technology Stack

- **Python** — core scripting and orchestration
- **PySpark** — distributed data processing
- **Spark SQL** — SQL-based querying on distributed datasets
- **Databricks** (Community Edition) — cloud platform for running the pipeline
- **Delta Lake** — ACID-compliant storage layer for Bronze/Silver/Gold tables

## Project Structure

```
smart-fraud-detection-pipeline/
├── data/raw/              Raw source CSVs (accounts, transactions, fraud watchlist)
├── notebooks/             Databricks notebooks (01_bronze, 02_silver, 03_gold)
├── src/
│   ├── schemas.py         Explicit StructType schemas for all 3 source CSVs
│   ├── bronze.py          Raw ingestion into Delta (Bronze layer)
│   ├── silver.py          Cleaning, enrichment, joins (Silver layer)
│   ├── gold.py             Fraud risk classification + summary tables (Gold layer)
│   ├── data_quality.py    Generates data_quality_report.md from Bronze/Silver
│   └── reporting.py       Builds and exports all Gold-layer insight reports
├── tests/                 Unit tests (fraud logic + reporting functions)
├── config/                pipeline_config.yaml — paths, thresholds, environment switch
├── docs/                  Project report
├── outputs/               Generated reports and exported insights (see below)
├── requirements.txt
└── run_pipeline.py        Single entrypoint: runs Bronze → Silver → Gold end-to-end (local)
```

## Configuration & Environments

`config/pipeline_config.yaml` starts with:

```yaml
environment: local   # databricks | local — controls which path set is used
```

Every path in the config (`source`, `tables`, `output`) has a matching `local` and `databricks` entry. Set `environment` to match where you're running — nothing else in the code needs to change. Exact paths used for each:

| | Local | Databricks |
|---|---|---|
| **Source CSVs** | `data/raw/accounts.csv`<br>`data/raw/transactions.csv`<br>`data/raw/known_fraud_accounts.csv` | `/Volumes/workspace/default/smart-fraud-detection-pipeline-data/accounts.csv`<br>`/Volumes/workspace/default/smart-fraud-detection-pipeline-data/transactions.csv`<br>`/Volumes/workspace/default/smart-fraud-detection-pipeline-data/known_fraud_accounts.csv` |
| **Bronze Delta tables** | `delta/bronze/` | `/Volumes/workspace/default/smart-fraud-detection-pipeline-data/delta/bronze/` |
| **Silver Delta table** | `delta/silver/` | `/Volumes/workspace/default/smart-fraud-detection-pipeline-data/delta/silver/` |
| **Gold Delta tables** | `delta/gold/` | `/Volumes/workspace/default/smart-fraud-detection-pipeline-data/delta/gold/` |
| **Report/insight outputs** | `outputs/` | `/Volumes/workspace/default/smart-fraud-detection-pipeline-data/outputs/` |

## Outputs

Running the pipeline (locally or on Databricks) produces the following files in `outputs/`:

**Core exports**
| File | Description |
|---|---|
| `gold_fraud_transactions_full.csv` | Full enriched + flagged transaction-level data |
| `fraud_summary_report.csv` | Fraud rate by branch and account type |
| `data_quality_report.md` | Nulls found, duplicates removed, orphan transactions, row counts |

**Gold layer insights**
| File | Description |
|---|---|
| `overall_summary.md` | Total transactions, confirmed_fraud/suspicious/normal counts and percentages, thresholds used |
| `risk_level_counts.csv` | Same risk-level breakdown as `overall_summary.md`, as a flat file |
| `account_breakdown_full.csv` | Every flagged account, with flagged transaction count and total flagged amount |
| `top_10_flagged_accounts.csv` | Top 10 accounts by total flagged amount |
| `top_10_flagged_merchants.csv` | Top 10 merchants by flagged transaction count |
| `fraud_type_breakdown.csv` | Count, amount, and distinct accounts per `fraud_type` from the watchlist |
| `monthly_fraud_trend.csv` | Total/confirmed/suspicious counts by transaction month |
| `city_fraud_breakdown.csv` | Fraud rate by transaction city |
| `kyc_risk_breakdown.csv` | Fraud rate by account KYC status |
| `international_vs_domestic.csv` | Fraud rate comparison between international and domestic transactions |

## Author

Shaik Faizan Ahmed
23B81A05L3
IV YEAR CSE
CVR COLLEGE OF ENGINEERING
