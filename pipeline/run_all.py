"""
Pipeline entry point — called by Docker CMD.

Runs the full medallion pipeline:
    Bronze (ingest) → Silver (transform) → Gold (provision)

Exit codes:
    0 = success (scoring system reads outputs)
    1 = failure (scoring system records zero correctness points)

WHY sys.exit(0) explicitly?
    Python exits 0 by default on clean completion, but being explicit
    means a future developer won't accidentally swallow an exception and
    exit 0 on failure. Explicit > implicit here.
"""

import sys
import logging
import traceback

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PIPELINE] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

from pipeline.ingest import run_ingestion
from pipeline.transform import run_transformation
from pipeline.provision import run_provisioning


if __name__ == "__main__":
    try:
        log.info("=" * 60)
        log.info("nyaks-medallion pipeline starting")
        log.info("=" * 60)

        log.info("Stage 1/3 — Bronze ingestion")
        run_ingestion()

        log.info("Stage 2/3 — Silver transformation")
        run_transformation()

        log.info("Stage 3/3 — Gold provisioning")
        run_provisioning()

        log.info("=" * 60)
        log.info("Pipeline complete. Exiting 0.")
        log.info("=" * 60)
        sys.exit(0)

    except Exception as e:
        log.error("Pipeline FAILED:")
        log.error(traceback.format_exc())
        sys.exit(1)