"""
Gold Layer — Dimensional model for analytics and AI workloads.

TABLES:
    dim_customers     (9 fields)
    dim_accounts      (11 fields)
    fact_transactions (15 fields)

SURROGATE KEY STRATEGY: sha2 hash — no window, no shuffle, stable across runs.
"""

import os
import logging
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, LongType

from pipeline.utils import load_config, get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s [GOLD] %(message)s")
log = logging.getLogger(__name__)


def _make_sk(col_name: str):
    """
    Generate a stable, unique surrogate key from a natural key column.

    WHY sha2 hash instead of row_number()?
        row_number() with no partitionBy moves ALL data to a single partition
        for global sorting — O(n log n) shuffle across one CPU core.
        At Stage 2 scale (3M transactions) this was timing out.

        sha2 is computed per-row with zero shuffle — O(n) with full parallelism.
        Both approaches are explicitly listed as acceptable in output_schema_spec.md.

    WHY substr(1, 15)?
        sha2 returns 64 hex chars. 15 hex chars = 60 bits.
        BIGINT is 64-bit signed, max ~9.2 * 10^18.
        15 hex chars max = 16^15 - 1 ≈ 1.15 * 10^18 — safely within BIGINT range.

    WHY conv(..., 16, 10)?
        sha2 returns a hex STRING. conv() converts from base 16 to base 10.
        We then cast to LongType (BIGINT) as the schema requires.
    """
    return (
        F.conv(F.sha2(F.col(col_name), 256).substr(1, 15), 16, 10)
        .cast(LongType())
    )


def _compute_age_band(df):
    """
    Derive age_band from dob using today as run date.

    WHY 365.25? Accounts for leap years. Using 365 causes off-by-one errors
    near birthday boundaries.
    """
    run_date = str(date.today())
    df = df.withColumn(
        "_age",
        F.floor(F.datediff(F.lit(run_date), F.col("dob")) / 365.25).cast(IntegerType())
    )
    return df.withColumn(
        "age_band",
        F.when(F.col("_age") >= 65, "65+")
         .when(F.col("_age") >= 56, "56-65")
         .when(F.col("_age") >= 46, "46-55")
         .when(F.col("_age") >= 36, "36-45")
         .when(F.col("_age") >= 26, "26-35")
         .when(F.col("_age") >= 18, "18-25")
         .otherwise(None)
    ).drop("_age")


def _build_dim_customers(spark, silver_path: str, gold_path: str):
    log.info("Building dim_customers ...")
    df = spark.read.format("delta").load(f"{silver_path}/customers")

    df = df.withColumn("customer_sk", _make_sk("customer_id"))
    df = _compute_age_band(df)

    dim = df.select(
        "customer_sk", "customer_id", "gender", "province",
        "income_band", "segment",
        F.col("risk_score").cast(IntegerType()),
        "kyc_status", "age_band"
    )

    out = f"{gold_path}/dim_customers"
    os.makedirs(out, exist_ok=True)
    dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)
    log.info(f"  dim_customers: {dim.count():,} rows")
    return dim


def _build_dim_accounts(spark, silver_path: str, gold_path: str):
    log.info("Building dim_accounts ...")
    df = spark.read.format("delta").load(f"{silver_path}/accounts")

    df = df.withColumn("account_sk", _make_sk("account_id"))

    # CRITICAL — GAP-026: rename customer_ref → customer_id
    # Validation Query 2 joins dim_accounts.customer_id = dim_customers.customer_id
    df = df.withColumnRenamed("customer_ref", "customer_id")

    dim = df.select(
        "account_sk", "account_id", "customer_id",
        "account_type", "account_status", "open_date", "product_tier",
        "digital_channel",
        F.col("credit_limit").cast(DecimalType(18, 2)),
        F.col("current_balance").cast(DecimalType(18, 2)),
        "last_activity_date"
    )

    out = f"{gold_path}/dim_accounts"
    os.makedirs(out, exist_ok=True)
    dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)
    log.info(f"  dim_accounts: {dim.count():,} rows")
    return dim


def _build_fact_transactions(spark, silver_path: str, gold_path: str,
                              dim_accounts, dim_customers) -> None:
    log.info("Building fact_transactions ...")
    txn = spark.read.format("delta").load(f"{silver_path}/transactions")

    # Lean lookups — only columns needed for FK resolution
    acct_lookup = dim_accounts.select("account_id", "account_sk", "customer_id")
    cust_lookup  = dim_customers.select("customer_id", "customer_sk")

    fact = (
        txn
        .join(acct_lookup, on="account_id", how="left")
        .join(cust_lookup,  on="customer_id",  how="left")
    )

    fact = fact.withColumn("transaction_sk", _make_sk("transaction_id"))

    fact_out = fact.select(
        "transaction_sk", "transaction_id", "account_sk", "customer_sk",
        "transaction_date", "transaction_timestamp", "transaction_type",
        "merchant_category", "merchant_subcategory",
        F.col("amount").cast(DecimalType(18, 2)),
        "currency", "channel", "province", "dq_flag", "ingestion_timestamp"
    )

    out = f"{gold_path}/fact_transactions"
    os.makedirs(out, exist_ok=True)
    fact_out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)
    log.info(f"  fact_transactions: {fact_out.count():,} rows")


def run_provisioning() -> None:
    config = load_config()
    spark  = get_spark(config)
    silver = config["output"]["silver_path"]
    gold   = config["output"]["gold_path"]

    dim_customers = _build_dim_customers(spark, silver, gold)
    dim_accounts  = _build_dim_accounts(spark, silver, gold)
    _build_fact_transactions(spark, silver, gold, dim_accounts, dim_customers)

    log.info("Gold provisioning complete.")