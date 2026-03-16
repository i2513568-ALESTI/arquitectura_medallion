"""
Bronze layer: load raw tables from the bronze schema in Supabase.
"""
import logging
from typing import Dict

import pandas as pd

from config.settings import settings
from config.database import get_engine

logger = logging.getLogger(__name__)


def load_bronze(tables: list[str] | None = None) -> Dict[str, pd.DataFrame]:
    """
    Load all (or specified) tables from the bronze schema.
    Returns a dict mapping table_name -> DataFrame.
    """
    tables = tables or settings.bronze_tables
    engine = get_engine()
    result = {}

    with engine.connect() as conn:
        for table in tables:
            try:
                query = f"SELECT * FROM {settings.bronze_schema}.{table}"
                df = pd.read_sql(query, conn)
                result[table] = df
                logger.info("Bronze loaded: %s (%d rows)", table, len(df))
            except Exception as e:
                logger.error("Failed to load bronze.%s: %s", table, e)
                raise

    return result
