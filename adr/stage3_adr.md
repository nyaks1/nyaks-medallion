# Architecture Decision Record: Stage 3 Streaming Extension

**File:** `adr/stage3_adr.md`
**Author:** Nyakallo Masiu
**Date:** 2026-05-02
**Status:** Final

---

## Context

Stage 3 requires adding a streaming ingestion path to a pipeline designed and submitted as a batch system at Stages 1 and 2. The mobile product team needs current balances and recent transactions updated within 5 minutes of a transaction event — the daily batch pipeline cannot satisfy this SLA.

The streaming interface delivers 12 pre-staged JSONL micro-batch files to `/data/stream/`, each containing 50–500 transaction events. The pipeline must poll this directory, process files in chronological order, and maintain two Gold output tables: `current_balances` (one row per account, upsert semantics) and `recent_transactions` (last 50 transactions per account, merge/evict semantics). Both tables must reflect events within 300 seconds of the source event timestamp for full SLA credit.

At the start of Stage 3, the pipeline consisted of five modules: `utils.py` (~60 lines), `ingest.py` (~80 lines), `transform.py` (~160 lines), `provision.py` (~180 lines), and `run_all.py` (~40 lines). The Stage 1 to Stage 2 transition added `dq_rules.yaml`, DQ flagging logic in `transform.py`, and `_write_dq_report()` in `provision.py` without modifying any existing function signatures — the diff was additive, not structural.

---

## Decision 1: How did your existing Stage 1 architecture facilitate or hinder the streaming extension?

**What made Stage 3 easier:**

The config-driven path setup in `pipeline_config.yaml` made adding new paths straightforward. At Stage 1, all input and output paths were externalised under `input:`, `output:`, and `spark:` keys. Adding `streaming.stream_input_path` and `streaming.stream_gold_path` required editing only the config file — no path strings needed to change in pipeline code. This is the direct payoff of the design principle stated in the brief: "designs that hardcode file paths will require more rework."

The Delta Lake session configuration established in `utils.py` — JAR loading, the DeltaSparkSessionExtension, the DeltaCatalog — transfers directly to the streaming path without modification. `get_spark()` returns a session that can write Delta MERGE operations the streaming path requires, because Delta support was wired at the session level, not per-table. The streaming path calls `get_spark()` and gets a fully capable session.

The hash-based surrogate key function `_make_sk()` in `provision.py` is stateless and row-level — it computes a deterministic BIGINT from any string column with zero shuffle. This means it works identically on a 1M-row batch DataFrame and on a 50-row streaming micro-batch. If surrogate keys had used `row_number()` over a global window, the streaming path would have required a completely different key generation approach.

**What made Stage 3 harder:**

The single `run_all.py` entry point calls `run_ingestion()`, `run_transformation()`, and `run_provisioning()` sequentially and exits. There is no concept of a long-running process or a polling loop. Adding Stage 3 required introducing a fourth execution concern that must run after the batch path completes — a concern the original entry point was not designed to accommodate. The `if __name__ == "__main__"` block needed a new branching conditional, which makes the entry point structurally more complex and harder to test independently.

The DQ flagging logic in `_transform_transactions()` is tightly coupled to a full Bronze Delta read. The streaming path processes micro-batches of 50–500 events that also need currency normalisation and type casting. Reusing `_transform_transactions()` directly is not possible — it reads from `/data/output/bronze/transactions/`, not from an in-memory DataFrame. A streaming-specific normalisation path duplicates the logic unless it is first extracted to a shared helper.

**Code survival rate:**

`utils.py`, `ingest.py`, and `run_all.py` survive intact — approximately 35% of total line count. `transform.py` and `provision.py` require additive changes only — new functions, no rewrites of existing ones. The new `stream_ingest.py` is entirely additive. Approximately 80% of Stage 1/2 code survives into Stage 3 without modification.

---

## Decision 2: What design decisions in Stage 1 would you change in hindsight?

**Extract a stateless normalisation function from `_transform_transactions()` in `transform.py`:**

Currently the currency normalisation, date parsing, and amount casting logic is embedded inline inside `_transform_transactions()`, which starts with a Bronze Delta read. For Stage 3, the same normalisation needs to run on streaming micro-batch DataFrames. If I had extracted a `_normalise_transaction_df(df: DataFrame) -> DataFrame` function that accepts any DataFrame and returns a normalised one, both the batch path and the streaming path would call the same function. `_transform_transactions()` would simply call `_normalise_transaction_df(df)` after the Bronze read. The streaming path would call `_normalise_transaction_df(micro_batch_df)` directly. No duplication, no divergence between batch and stream normalisation logic.

**Design `run_all.py` with a `--mode` argument from Day 1:**

The current entry point has no argument parsing — it runs the full pipeline unconditionally. If I had added `import argparse` and a `--mode` flag accepting `batch`, `stream`, or `full` from the start, adding Stage 3 would have been one new branch in the main block and one new function call. Instead, Stage 3 required restructuring the entry point itself. A concrete example: `python pipeline/run_all.py --mode stream` would run only the polling loop, making it independently testable without running the full batch pipeline first. This would have saved significant debugging time.

**Use Delta MERGE for Gold writes instead of overwrite in `provision.py`:**

Every Gold table write in `provision.py` uses `.mode("overwrite")`. This is correct for daily batch loads but creates a gap at Stage 3: `current_balances` requires upsert semantics — merge if account exists, insert if not. If all Gold writes had used Delta MERGE from Day 1, the streaming path would reuse the exact same write pattern. Switching from overwrite to MERGE at Stage 3 means modifying `_build_dim_customers()`, `_build_dim_accounts()`, and `_build_fact_transactions()` retroactively — three functions that were otherwise stable.

---

## Decision 3: How would you approach this differently if you had known Stage 3 was coming from the start?

**Stateless normalisation layer shared by batch and stream:**

From Day 1, I would have separated data normalisation from data movement. `transform.py` would expose a `normalise_transactions(df)` function that takes any DataFrame and returns a cleaned one — no Bronze reads, no Silver writes, just transformations. The batch path would call it as part of `_transform_transactions()`. The streaming path would call it on each micro-batch. This single change eliminates the biggest source of streaming friction without adding any complexity to Stage 1 or Stage 2.

**Delta MERGE as the default write pattern:**

If I had known Stage 3 required upsert semantics, I would have built a `_merge_to_delta(df, path, merge_key)` utility in `utils.py` from the start. The batch pipeline would call it with the full Silver dataset on each run — idempotent, safe for re-runs, identical behaviour to overwrite for a fresh table. The streaming pipeline would call it with each micro-batch. One write utility, two callers, no duplication. The `current_balances` upsert and `recent_transactions` merge would be implemented as thin wrappers around the same utility.

**Single entry point with explicit mode selection and shared library code:**

The architecture I would have chosen from Day 1: `run_all.py` accepts `--mode batch|stream|full`. Batch mode runs ingest → transform → provision and exits. Stream mode starts the polling loop, processes all files in `/data/stream/`, and exits after a configurable quiesce timeout. Full mode runs batch first, then hands off to the stream loop. All shared logic — SparkSession, DQ normalisation, Delta writes — lives in the existing modules and is imported by both modes. The entry point stays thin: argument parsing and orchestration only. This makes each mode independently testable, independently invocable, and independently debuggable — which at Stage 3 with a 30-minute hard timeout is not a luxury but a necessity.