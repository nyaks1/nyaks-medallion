"""
Gold layer: Join and aggregate Silver tables into the scored output schema.

Input paths (Silver layer output — read these, do not modify):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Output paths (your pipeline must create these directories):
  /data/output/gold/fact_transactions/     — 15 fields (see output_schema_spec.md §2)
  /data/output/gold/dim_accounts/          — 11 fields (see output_schema_spec.md §3)
  /data/output/gold/dim_customers/         — 9 fields  (see output_schema_spec.md §4)

Requirements:
  - Generate surrogate keys (_sk fields) that are unique, non-null, and stable
    across pipeline re-runs on the same input data. Use row_number() with a
    stable ORDER BY on the natural key, or sha2(natural_key, 256) cast to BIGINT.
  - Resolve all foreign key relationships:
      fact_transactions.account_sk  → dim_accounts.account_sk
      fact_transactions.customer_sk → dim_customers.customer_sk
      dim_accounts.customer_id      → dim_customers.customer_id
  - Rename accounts.customer_ref → dim_accounts.customer_id at this layer.
  - Derive dim_customers.age_band from dob (do not copy dob directly).
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.
  - At Stage 2, also write /data/output/dq_report.json summarising DQ outcomes.

See output_schema_spec.md for the complete field-by-field specification.
"""


"""
Gold Layer — Dimensional model for analytics and AI workloads.

TABLES:
    dim_customers   (9 fields)   — one row per customer
    dim_accounts    (11 fields)  — one row per account
    fact_transactions (15 fields) — one row per transaction

VALIDATION QUERIES this code must satisfy:
    Q1: fact_transactions grouped by transaction_type → 4 rows
    Q2: dim_accounts LEFT JOIN dim_customers ON customer_id → 0 unlinked
    Q3: dim_customers JOIN dim_accounts grouped by province → 9 rows

KEY: Q2 fails if dim_accounts.customer_id is missing or mis-named.
     It comes from accounts.customer_ref — renamed at Gold layer (GAP-026).
"""

import os
import logging
from datetime import date

from pyspark.sql import Window, functions as F
from pyspark.sql.types import DecimalType, IntegerType

from pipeline.utils import load_config, get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s [GOLD] %(message)s")
log = logging.getLogger(__name__)


def _compute_age_band(df):
    """
    Derive age_band from dob using today as run date.

    WHY not copy dob directly?
        The spec says age_band is derived — raw dob must NOT appear in
        dim_customers output. Also: age band is less sensitive than exact DOB.

    WHY 365.25?
        Accounts for leap years. Using 365 causes off-by-one errors near
        birthday boundaries — candidates who skip this will have edge-case
        failures the scoring harness may catch.

    Buckets: 18-25, 26-35, 36-45, 46-55, 56-65, 65+
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

    # Surrogate key — row_number() on natural key = stable across re-runs
    window = Window.orderBy("customer_id")
    df = df.withColumn("customer_sk", F.row_number().over(window))
    df = _compute_age_band(df)

    # Exactly 9 fields in spec order
    dim = df.select(
        "customer_sk", "customer_id", "gender", "province",
        "income_band", "segment",
        F.col("risk_score").cast(IntegerType()),
        "kyc_status", "age_band"
    )

    out = f"{gold_path}/dim_customers"
    os.makedirs(out, exist_ok=True)
    dim.write.format("delta").mode("overwrite").save(out)
    log.info(f"  dim_customers: {dim.count():,} rows")
    return dim


def _build_dim_accounts(spark, silver_path: str, gold_path: str):
    log.info("Building dim_accounts ...")
    df = spark.read.format("delta").load(f"{silver_path}/accounts")

    window = Window.orderBy("account_id")
    df = df.withColumn("account_sk", F.row_number().over(window))

    # CRITICAL — GAP-026: rename customer_ref → customer_id
    # Validation Query 2 joins: dim_accounts.customer_id = dim_customers.customer_id
    # If this rename doesn't happen, Query 2 returns zero matches → 0 points.
    df = df.withColumnRenamed("customer_ref", "customer_id")

    # Exactly 11 fields in spec order
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
    dim.write.format("delta").mode("overwrite").save(out)
    log.info(f"  dim_accounts: {dim.count():,} rows")
    return dim


def _build_fact_transactions(spark, silver_path: str, gold_path: str,
                              dim_accounts, dim_customers) -> None:
    log.info("Building fact_transactions ...")
    txn = spark.read.format("delta").load(f"{silver_path}/transactions")

    # Lean lookups — only pull the FK columns we need, nothing extra
    # WHY? Joining fat DataFrames shuffles more data across partitions.
    acct_lookup = dim_accounts.select("account_id", "account_sk", "customer_id")
    cust_lookup  = dim_customers.select("customer_id", "customer_sk")

    # LEFT JOIN so Stage 2 orphaned transactions still appear (with null FKs)
    # Stage 2: those rows get dq_flag = ORPHANED_ACCOUNT
    fact = (
        txn
        .join(acct_lookup, on="account_id", how="left")
        .join(cust_lookup,  on="customer_id",  how="left")
    )

    window = Window.orderBy("transaction_id")
    fact = fact.withColumn("transaction_sk", F.row_number().over(window))

    # Exactly 15 fields in spec order
    fact_out = fact.select(
        "transaction_sk", "transaction_id", "account_sk", "customer_sk",
        "transaction_date", "transaction_timestamp", "transaction_type",
        "merchant_category", "merchant_subcategory",
        F.col("amount").cast(DecimalType(18, 2)),
        "currency", "channel", "province", "dq_flag", "ingestion_timestamp"
    )

    out = f"{gold_path}/fact_transactions"
    os.makedirs(out, exist_ok=True)
    fact_out.write.format("delta").mode("overwrite").save(out)
    log.info(f"  fact_transactions: {fact_out.count():,} rows")


def run_provisioning() -> None:
    config = load_config()
    spark  = get_spark(config)
    silver = config["output"]["silver_path"]
    gold   = config["output"]["gold_path"]

    # Order is mandatory: customers → accounts → transactions
    # Each step depends on the lookups produced by the previous.
    dim_customers = _build_dim_customers(spark, silver, gold)
    dim_accounts  = _build_dim_accounts(spark, silver, gold)
    _build_fact_transactions(spark, silver, gold, dim_accounts, dim_customers)

    log.info("Gold provisioning complete.")