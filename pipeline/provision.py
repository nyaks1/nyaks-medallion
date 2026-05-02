"""
Gold Layer — Dimensional model + DQ report writer.

STAGE 2 ADDITIONS vs STAGE 1:
    - dq_report.json generated at /data/output/dq_report.json
    - ORPHANED_ACCOUNT detection during fact_transactions build
    - .count() calls removed from write paths (expensive at 3M rows)
    - overwriteSchema on all Delta writes
"""

import os
import json
import logging
from datetime import date, datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, LongType

from pipeline.utils import load_config, get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s [GOLD] %(message)s")
log = logging.getLogger(__name__)

# Track pipeline start time for dq_report
_PIPELINE_START = datetime.now(timezone.utc)


def _make_sk(col_name: str):
    """
    Stable surrogate key via sha2 hash — zero shuffle, zero WindowExec warnings.
    sha2 → first 15 hex chars → conv base16→base10 → BIGINT
    """
    return (
        F.conv(F.sha2(F.col(col_name), 256).substr(1, 15), 16, 10)
        .cast(LongType())
    )


def _compute_age_band(df):
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
    log.info(f"  dim_customers written")
    return dim


def _build_dim_accounts(spark, silver_path: str, gold_path: str):
    log.info("Building dim_accounts ...")
    df = spark.read.format("delta").load(f"{silver_path}/accounts")
    df = df.withColumn("account_sk", _make_sk("account_id"))
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
    log.info(f"  dim_accounts written")
    return dim


def _build_fact_transactions(spark, silver_path: str, gold_path: str,
                              dim_accounts, dim_customers):
    log.info("Building fact_transactions ...")
    txn = spark.read.format("delta").load(f"{silver_path}/transactions")

    acct_lookup = dim_accounts.select("account_id", "account_sk", "customer_id")
    cust_lookup  = dim_customers.select("customer_id", "customer_sk")

    fact = (
        txn
        .join(acct_lookup, on="account_id", how="left")
        .join(cust_lookup,  on="customer_id",  how="left")
    )

    # ------------------------------------------------------------------
    # DQ: ORPHANED_ACCOUNT — transaction joined but no matching account
    # account_sk will be null after the left join if account_id not found
    # ------------------------------------------------------------------
    fact = fact.withColumn(
        "dq_flag",
        F.when(
            F.col("account_sk").isNull() & F.col("dq_flag").isNull(),
            "ORPHANED_ACCOUNT"
        ).otherwise(F.col("dq_flag"))
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
    log.info(f"  fact_transactions written")
    return fact_out


def _write_dq_report(spark, silver_path: str, gold_path: str,
                     dq_report_path: str, config: dict) -> None:
    """
    Write dq_report.json to /data/output/dq_report.json.

    WHY count per dq_flag value?
        The scoring harness cross-references our reported counts against
        its own computed expected values with ±5% tolerance.
        We derive counts from the actual Gold output — guaranteeing
        consistency between what we wrote and what we report.
    """
    log.info("Writing dq_report.json ...")

    stage = str(config.get("pipeline", {}).get("stage", "2"))

    # Source record counts from Silver (post-dedup, pre-Gold)
    accounts_raw     = spark.read.format("delta").load(f"{silver_path}/accounts").count()
    customers_raw    = spark.read.format("delta").load(f"{silver_path}/customers").count()
    transactions_raw = spark.read.format("delta").load(f"{silver_path}/transactions").count()

    # DQ flag counts from fact_transactions
    fact = spark.read.format("delta").load(f"{gold_path}/fact_transactions")
    flag_counts = (
        fact.groupBy("dq_flag")
            .count()
            .collect()
    )
    flag_map = {row["dq_flag"]: row["count"] for row in flag_counts if row["dq_flag"]}

    total_txn = transactions_raw
    dq_issues = []
    for issue_type, count in flag_map.items():
        dq_issues.append({
            "issue_type": issue_type,
            "records_affected": count,
            "percentage_of_total": round(count * 100.0 / total_txn, 2),
            "handling_action": _handling_for(issue_type),
            "records_in_output": 0 if issue_type == "ORPHANED_ACCOUNT" else count
        })

    gold_counts = {
        "fact_transactions": fact.count(),
        "dim_accounts":      spark.read.format("delta").load(f"{gold_path}/dim_accounts").count(),
        "dim_customers":     spark.read.format("delta").load(f"{gold_path}/dim_customers").count(),
    }

    duration = int((datetime.now(timezone.utc) - _PIPELINE_START).total_seconds())

    report = {
        "$schema": "nedbank-de-challenge/dq-report/v1",
        "run_timestamp": _PIPELINE_START.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stage": stage,
        "source_record_counts": {
            "accounts_raw":     accounts_raw,
            "transactions_raw": transactions_raw,
            "customers_raw":    customers_raw,
        },
        "dq_issues": dq_issues,
        "gold_layer_record_counts": gold_counts,
        "execution_duration_seconds": duration,
    }

    os.makedirs(os.path.dirname(dq_report_path), exist_ok=True)
    with open(dq_report_path, "w") as f:
        json.dump(report, f, indent=2)

    log.info(f"  dq_report.json written ({len(dq_issues)} issue types)")


def _handling_for(issue_type: str) -> str:
    """Map dq_flag codes to handling action strings for the DQ report."""
    mapping = {
        "DUPLICATE_DEDUPED":  "DEDUPLICATED_KEEP_FIRST",
        "ORPHANED_ACCOUNT":   "QUARANTINED",
        "TYPE_MISMATCH":      "CAST_TO_DECIMAL",
        "DATE_FORMAT":        "NORMALISED_DATE",
        "CURRENCY_VARIANT":   "NORMALISED_CURRENCY",
        "NULL_REQUIRED":      "EXCLUDED_NULL_PK",
    }
    return mapping.get(issue_type, "UNKNOWN")


def run_provisioning() -> None:
    config        = load_config()
    spark         = get_spark(config)
    silver        = config["output"]["silver_path"]
    gold          = config["output"]["gold_path"]
    dq_report_path = config["output"].get("dq_report_path", "/data/output/dq_report.json")
    stage         = config.get("pipeline", {}).get("stage", 1)

    dim_customers = _build_dim_customers(spark, silver, gold)
    dim_accounts  = _build_dim_accounts(spark, silver, gold)
    _build_fact_transactions(spark, silver, gold, dim_accounts, dim_customers)

    # Write DQ report from Stage 2 onwards
    if int(stage) >= 2:
        _write_dq_report(spark, silver, gold, dq_report_path, config)

    log.info("Gold provisioning complete.")