from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

# Dates stay as strings here, we cast them properly in the silver layer.

# accounts.csv
ACCOUNTS_SCHEMA = StructType([
    StructField("account_id", StringType(), nullable=False),
    StructField("customer_name", StringType(), nullable=True),
    StructField("account_type", StringType(), nullable=True),
    StructField("opening_date", StringType(), nullable=True),
    StructField("branch", StringType(), nullable=True),
    StructField("kyc_status", StringType(), nullable=True),
    StructField("credit_limit", DoubleType(), nullable=True),
])

# transactions.csv
TRANSACTIONS_SCHEMA = StructType([
    StructField("txn_id", StringType(), nullable=False),
    StructField("account_id", StringType(), nullable=False),
    StructField("txn_date", StringType(), nullable=True),
    StructField("txn_type", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("merchant", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("is_international", StringType(), nullable=True),
])

# known_fraud_accounts.csv
FRAUD_WATCHLIST_SCHEMA = StructType([
    StructField("account_id", StringType(), nullable=False),
    StructField("fraud_type", StringType(), nullable=True),
    StructField("flagged_date", StringType(), nullable=True),
])

SCHEMA_REGISTRY = {
    "accounts": ACCOUNTS_SCHEMA,
    "transactions": TRANSACTIONS_SCHEMA,
    "fraud_watchlist": FRAUD_WATCHLIST_SCHEMA,
}
