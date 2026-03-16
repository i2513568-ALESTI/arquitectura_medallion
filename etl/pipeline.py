"""
Orchestrates the full ETL: Bronze → Silver → Gold, create schemas, write to Supabase.
"""
import logging
from typing import Dict

import pandas as pd
from config.settings import settings
from config.database import get_connection, get_engine
from etl.bronze import load_bronze
from etl.silver import transform_to_silver
from etl.gold import build_gold

logger = logging.getLogger(__name__)


def _ensure_schemas(conn) -> None:
    """Create silver and gold schemas if they do not exist."""
    cur = conn.cursor()
    try:
        for schema in (settings.silver_schema, settings.gold_schema):
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            conn.commit()
            logger.info("Schema %s ready", schema)
    except Exception as e:
        conn.rollback()
        logger.exception("Failed to create schema: %s", e)
        raise
    finally:
        cur.close()


def _write_tables(
    data: Dict[str, pd.DataFrame],
    schema: str,
    engine,
    if_exists: str = "replace",
) -> None:
    """Write each DataFrame to schema.table_name using pandas.to_sql."""
    for table_name, df in data.items():
        try:
            df.to_sql(
                table_name,
                engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method="multi",
                chunksize=1000,
            )
            logger.info("Written %s.%s (%d rows)", schema, table_name, len(df))
        except Exception as e:
            logger.exception("Failed to write %s.%s: %s", schema, table_name, e)
            raise


def run_pipeline(
    bronze_tables: list[str] | None = None,
    silver_if_exists: str = "replace",
    gold_if_exists: str = "replace",
) -> Dict[str, Dict[str, pd.DataFrame]]:
    """
    Run the full Medallion ETL pipeline:
    1. Load bronze tables from Supabase
    2. Transform to silver (clean)
    3. Build gold (aggregated by id_asesor)
    4. Create schemas silver and gold if missing
    5. Write silver and gold to Supabase via pandas.to_sql

    Returns dict with keys "bronze", "silver", "gold" (each a dict of name -> DataFrame).
    """
    logger.info("Starting ETL pipeline (Bronze -> Silver -> Gold)")

    # 1. Bronze
    data_bronze = load_bronze(tables=bronze_tables)
    if not data_bronze:
        raise ValueError("No bronze data loaded")

    # 2. Silver
    data_silver = transform_to_silver(data_bronze)

    # 3. Gold
    data_gold = build_gold(data_silver)

    # 4. Ensure schemas
    conn = get_connection()
    try:
        _ensure_schemas(conn)
    finally:
        conn.close()

    # 5. Write to Supabase
    engine = get_engine()
    _write_tables(data_silver, settings.silver_schema, engine, if_exists=silver_if_exists)
    _write_tables(data_gold, settings.gold_schema, engine, if_exists=gold_if_exists)

    logger.info("ETL pipeline completed successfully")
    return {
        "bronze": data_bronze,
        "silver": data_silver,
        "gold": data_gold,
    }
