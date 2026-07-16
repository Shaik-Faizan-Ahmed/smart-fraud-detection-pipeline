# Smart Fraud Detection Pipeline

A scalable data engineering pipeline built with **PySpark**, **Spark SQL**, and **Delta Lake** on **Databricks**, designed to detect fraudulent financial transactions using the **Medallion Architecture** (Bronze → Silver → Gold).

This is a project for Celebal Excellence Intership by Shaik Faizan Ahmed (Data Engineering Intern) of CVR College of Engineering.
---

## Problem Statement

Financial institutions process large volumes of transactions daily, making it difficult to manually detect fraudulent activity. This project builds a data pipeline that ingests account, transaction, and fraud-watchlist data, cleans and enriches it, and applies fraud-detection logic to classify transactions and surface actionable insights.

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

## Datasets

| Dataset | Description |
|---|---|
| `accounts.csv` | Account holder details: account_id, customer_name, account_type, opening_date, branch, kyc_status, credit_limit |
| `transactions.csv` | Transaction records: txn_id, account_id, txn_date, txn_type, amount, merchant, city, is_international |
| `known_fraud_accounts.csv` | Fraud watchlist: account_id, fraud_type, flagged_date |

## Technology Stack

- **Python** — core scripting and orchestration
- **PySpark** — distributed data processing
- **Spark SQL** — SQL-based querying on distributed datasets
- **Databricks** (Community Edition) — cloud platform for running the pipeline
- **Delta Lake** — ACID-compliant storage layer for Bronze/Silver/Gold tables

## Project Structure

```
smart-fraud-detection-pipeline/
├── data/raw/              Raw source CSVs
├── notebooks/             Databricks notebooks (Bronze, Silver, Gold)
├── src/                   Reusable pipeline modules (schemas, bronze, silver, gold)
├── tests/                 Unit tests for fraud-detection logic
├── config/                Pipeline configuration (paths, thresholds, table names)
├── docs/                  Project report
├── outputs/               Sample pipeline outputs and insights
├── requirements.txt
└── run_pipeline.py        Single entrypoint: runs Bronze → Silver → Gold end-to-end
```

## Author

Shaik Faizan Ahmed
23B81A05L3
IV YEAR CSE
CVR COLLEGE OF ENGINEERING