# nyaks-medallion

Medallion data pipeline for fintech virtual account analytics — Bronze to Gold, POPIA-aware by design.

## Architecture

- **Bronze:** Raw Delta ingestion with ingestion_timestamp audit trail
- **Silver:** Type casting, deduplication, DQ flagging, currency/date normalisation  
- **Gold:** fact_transactions, dim_accounts, dim_customers — all three validation queries pass

## Stack
Python 3.11 · PySpark 3.5 · Delta Lake 3.1 · Docker

## Design decisions
- Hash-based surrogate keys (sha2) — zero shuffle, no WindowExec degradation
- Config-driven via `pipeline_config.yaml` — no hardcoded paths
- POPIA-aware — PII minimised at Silver, DOB never exposed in Gold, pseudonymised surrogate keys
- Stage 2-ready from Stage 1 — merchant_subcategory, currency variants, multi-format dates handled from day one

## Run locally
```bash
docker build -t nyaks-medallion:test .
docker run --rm --memory=2g --cpus=2 -v ${PWD}/data:/data nyaks-medallion:test
```