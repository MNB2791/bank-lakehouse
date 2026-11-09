from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities import transforms as T

CATALOG = spark.conf.get("bank_catalog", "bank_dev")
# Portfolio default. In production the salt comes from a secret, never plain config.
PII_SALT = spark.conf.get("pii_salt", "dev-only-salt")


def _scd1(name: str, key: str, source_view: str, comment: str):
    """Create a Silver streaming table and upsert (SCD type 1) from a cleaned view."""
    target = f"{CATALOG}.silver.{name}"
    dp.create_streaming_table(name=target, comment=comment, table_properties={"quality": "silver"})
    dp.create_auto_cdc_flow(
        target=target, source=source_view, keys=[key],
        sequence_by="_ingested_at", stored_as_scd_type=1,
    )


# ---------------------------------------------------------------- transactions
TXN_RULES = {
    "valid_txn_id": "transaction_id IS NOT NULL",
    "valid_account_id": "account_id IS NOT NULL",
    "valid_amount": "amount IS NOT NULL AND amount > 0",
    "valid_txn_type": "txn_type IS NOT NULL AND txn_type IN ('DEBIT','CREDIT','TRANSFER','WITHDRAWAL','PAYMENT')",
    "valid_timestamp": "txn_ts IS NOT NULL",
}
QUARANTINE_FILTER = "NOT coalesce(" + " AND ".join(f"({r})" for r in TXN_RULES.values()) + ", false)"


@dp.temporary_view(name="txn_typed")
def txn_typed():
    b = spark.readStream.table(f"{CATALOG}.bronze.transactions")
    return b.select(
        "transaction_id", "account_id",
        F.to_timestamp("txn_ts").alias("txn_ts"),
        F.upper("txn_type").alias("txn_type"),
        F.upper("channel").alias("channel"),
        F.col("amount").cast("decimal(18,2)").alias("amount"),
        "merchant_category",
        F.upper("merchant_country").alias("merchant_country"),
        F.col("fraud_flag").cast("int").alias("fraud_flag"),
        "_ingested_at",
    )


@dp.temporary_view(name="txn_clean")
@dp.expect_all_or_drop(TXN_RULES)                      # failing rows are dropped and counted
def txn_clean():
    t = spark.readStream.table("txn_typed")
    return (
        t.withColumn("txn_date", F.to_date("txn_ts"))
         .withColumn("direction", T.direction(F.col("txn_type")))
         .withColumn("signed_amount", T.signed_amount(F.col("amount"), F.col("direction")))
         .withColumn("is_foreign", F.col("merchant_country") != "US")
         .withColumn("is_high_value", F.col("amount") >= 1000)
    )


@dp.table(
    name=f"{CATALOG}.silver.transactions_quarantine",
    comment="Transactions that failed Silver rules, kept for data steward review",
)
def txn_quarantine():
    return spark.readStream.table("txn_typed").where(F.expr(QUARANTINE_FILTER))


_scd1("transactions", "transaction_id", "txn_clean", "Validated, deduplicated transactions")


# -------------------------------------------------------------------- customers
@dp.temporary_view(name="customers_clean")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_credit_score", "credit_score BETWEEN 300 AND 850")
@dp.expect("valid_email", "email LIKE '%@%.%'")        # warn only: keep the row, record the violation
@dp.expect("known_kyc_status", "kyc_status IN ('verified','pending','expired')")
def customers_clean():
    b = spark.readStream.table(f"{CATALOG}.bronze.customers")
    dob = F.to_date("date_of_birth")
    return b.select(
        "customer_id",
        F.initcap("first_name").alias("first_name"),
        F.initcap("last_name").alias("last_name"),
        F.lower("email").alias("email"),
        "phone",
        dob.alias("date_of_birth"),
        T.age_band(dob).alias("age_band"),
        T.hash_pii(F.col("ssn"), PII_SALT).alias("ssn_hash"),
        T.mask_ssn(F.col("ssn")).alias("ssn_masked"),
        F.upper("state").alias("state"),
        "segment",
        F.col("credit_score").cast("int").alias("credit_score"),
        "kyc_status",
        F.to_timestamp("created_at").alias("created_at"),
        "_ingested_at",
    )


_scd1("customers", "customer_id", "customers_clean", "Typed customers; SSN pseudonymized")


# --------------------------------------------------------------------- accounts
@dp.temporary_view(name="accounts_clean")
@dp.expect_or_drop("valid_account_id", "account_id IS NOT NULL")
@dp.expect_or_drop("has_customer", "customer_id IS NOT NULL")
@dp.expect("known_status", "status IN ('ACTIVE','DORMANT','CLOSED')")
@dp.expect("non_negative_opening_balance", "opening_balance >= 0")
def accounts_clean():
    b = spark.readStream.table(f"{CATALOG}.bronze.accounts")
    return b.select(
        "account_id", "customer_id", "branch_id",
        F.upper("account_type").alias("account_type"),
        F.upper("status").alias("status"),
        F.col("opening_balance").cast("decimal(18,2)").alias("opening_balance"),
        F.to_date("open_date").alias("open_date"),
        "_ingested_at",
    )


_scd1("accounts", "account_id", "accounts_clean", "Typed accounts")


# --------------------------------------------------------------------- branches
@dp.temporary_view(name="branches_clean")
@dp.expect_or_drop("valid_branch_id", "branch_id IS NOT NULL")
def branches_clean():
    b = spark.readStream.table(f"{CATALOG}.bronze.branches")
    return b.select(
        "branch_id", "branch_name", "region", F.upper("state").alias("state"),
        F.to_date("opened_date").alias("opened_date"), "_ingested_at",
    )


_scd1("branches", "branch_id", "branches_clean", "Typed branches")