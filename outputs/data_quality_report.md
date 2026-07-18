# Data Quality Report

Generated automatically by the pipeline. Summarizes what Bronze/Silver found and fixed.

## Source Row Counts
- Accounts: 50
- Transactions: 200

## Nulls Found in Bronze (before cleaning)
- Missing `credit_limit`: 9 accounts
- Missing `merchant`: 7 transactions

## Duplicates
- Duplicate `txn_id` rows removed: 0

## Silver Layer Result
- Enriched transaction rows: 200
- Orphan transactions (account_id not found in accounts.csv): 8
