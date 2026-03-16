"""
Gold layer: aggregated tables grouped by id_asesor.
"""
import logging
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)


def _aggregate_by_asesor(df: pd.DataFrame, table_name: str) -> pd.DataFrame | None:
    """
    Aggregate a fact table by id_asesor (sum of numeric columns).
    Returns None if table has no id_asesor or is empty.
    """
    if df is None or df.empty:
        return None
    if "id_asesor" not in df.columns:
        logger.debug("Table %s has no id_asesor, skipping aggregation", table_name)
        return None

    numeric_cols = [
        c for c in df.select_dtypes(include=["number"]).columns
        if c != "id_asesor"
    ]
    if not numeric_cols:
        agg = df.groupby("id_asesor", as_index=False).size().rename(columns={"size": "registros"})
    else:
        agg = df.groupby("id_asesor", as_index=False)[numeric_cols].sum()

    # Suffix column names to avoid clashes when merging
    suffix = table_name.replace("fact_", "")
    rename = {c: f"{c}_{suffix}" for c in agg.columns if c != "id_asesor"}
    agg = agg.rename(columns=rename)
    return agg


def build_gold(data_silver: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Build gold layer from silver:
    - dim_asesor: copy of silver dim_asesor (cleaned dimension)
    - resumen_por_asesor: one row per id_asesor with aggregated metrics from all facts
    """
    gold = {}

    # Gold dimension: as-is from silver
    if "dim_asesor" in data_silver:
        gold["dim_asesor"] = data_silver["dim_asesor"].copy()
        logger.info("Gold dim_asesor: %d rows", len(gold["dim_asesor"]))

    # Resumen por asesor: merge all fact aggregations
    fact_tables = [
        "fact_reclutamiento",
        "fact_rendimiento_mensual",
        "fact_capacitacion",
        "fact_calidad",
        "fact_incidencias",
        "fact_adherencia",
        "fact_clima",
    ]

    if "dim_asesor" in data_silver:
        resumen = data_silver["dim_asesor"][["id_asesor"]].drop_duplicates()
    else:
        resumen = pd.DataFrame(columns=["id_asesor"])

    for table_name in fact_tables:
        df = data_silver.get(table_name)
        agg = _aggregate_by_asesor(df, table_name)
        if agg is not None and not agg.empty:
            resumen = resumen.merge(agg, on="id_asesor", how="left")
            logger.debug("Gold resumen: merged %s", table_name)

    gold["resumen_por_asesor"] = resumen
    logger.info("Gold resumen_por_asesor: %d rows", len(resumen))

    return gold
