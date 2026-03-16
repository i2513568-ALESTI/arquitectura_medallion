"""
Silver layer: clean data (remove duplicates, trim strings, handle nulls).
"""
import logging
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply silver cleaning rules:
    - Remove duplicate rows
    - Trim string columns (strip whitespace)
    - Fill numeric nulls with 0; leave object nulls as empty string for consistency
    """
    out = df.copy()

    # Drop duplicates
    before = len(out)
    out = out.drop_duplicates()
    if len(out) < before:
        logger.debug("Dropped %d duplicate rows", before - len(out))

    # String/object columns: strip and replace NaN with empty string
    str_cols = out.select_dtypes(include=["object", "string"]).columns.tolist()
    for col in str_cols:
        out[col] = out[col].astype(str).str.strip().replace("nan", "")

    # Numeric columns: fill NaN with 0
    num_cols = out.select_dtypes(include=["number"]).columns.tolist()
    for col in num_cols:
        out[col] = out[col].fillna(0)

    return out


def transform_to_silver(data_bronze: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Transform bronze DataFrames to silver (cleaned).
    Returns a dict mapping table_name -> cleaned DataFrame.
    """
    result = {}
    for name, df in data_bronze.items():
        try:
            result[name] = _clean_dataframe(df)
            logger.info("Silver transformed: %s (%d rows)", name, len(result[name]))
        except Exception as e:
            logger.error("Silver transform failed for %s: %s", name, e)
            raise
    return result
