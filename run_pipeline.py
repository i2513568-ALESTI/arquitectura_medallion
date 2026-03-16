#!/usr/bin/env python3
"""
Production ETL pipeline: Bronze → Silver → Gold for Supabase (PostgreSQL).
Run from project root: python run_pipeline.py
"""
import logging
import sys
from pathlib import Path

# Ensure project root is on path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.pipeline import run_pipeline

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def main():
    try:
        result = run_pipeline(
            bronze_tables=None,  # use default from config
            silver_if_exists="replace",
            gold_if_exists="replace",
        )
        logger.info(
            "Pipeline finished. Bronze: %d tables, Silver: %d, Gold: %d",
            len(result["bronze"]),
            len(result["silver"]),
            len(result["gold"]),
        )
        return 0
    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
