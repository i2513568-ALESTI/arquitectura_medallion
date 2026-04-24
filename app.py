import pandas as pd
import streamlit as st
from sqlalchemy import inspect, text

from config.database import get_engine


st.set_page_config(
    page_title="SATDPO | Silver & Gold",
    page_icon="📊",
    layout="wide",
)

st.title("SATDPO - Explorador de datos (Silver / Gold)")
st.caption("Visor de tablas en Supabase/PostgreSQL con KPIs y control de calidad básico.")


@st.cache_resource(show_spinner=False)
def _engine():
    return get_engine()


def list_tables(schema: str) -> list[str]:
    eng = _engine()
    insp = inspect(eng)
    return sorted(insp.get_table_names(schema=schema))


@st.cache_data(ttl=60, show_spinner=False)
def get_table_rowcount(schema: str, table: str) -> int:
    eng = _engine()
    q = text(f'SELECT COUNT(*) AS n FROM "{schema}"."{table}"')
    return int(pd.read_sql(q, eng)["n"].iloc[0])


@st.cache_data(ttl=60, show_spinner=False)
def load_table_sample(schema: str, table: str, limit: int, offset: int) -> pd.DataFrame:
    eng = _engine()
    q = text(f'SELECT * FROM "{schema}"."{table}" LIMIT :limit OFFSET :offset')
    return pd.read_sql(q, eng, params={"limit": int(limit), "offset": int(offset)})


def nulls_profile(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["columna", "nulos", "pct_nulos"])
    n = len(df)
    s = df.isna().sum().sort_values(ascending=False)
    out = pd.DataFrame(
        {
            "columna": s.index,
            "nulos": s.values,
            "pct_nulos": (s.values / max(n, 1) * 100).round(2),
        }
    )
    return out


with st.sidebar:
    st.subheader("Navegación")
    schema = st.radio("Schema", options=["silver", "gold"], index=1)

    try:
        tables = list_tables(schema)
    except Exception as e:
        st.error(f"No pude listar tablas en `{schema}`. Error: {e}")
        st.stop()

    if not tables:
        st.warning(f"No hay tablas en `{schema}`.")
        st.stop()

    table = st.selectbox("Tabla", options=tables)
    page_size = st.select_slider("Filas por página", options=[50, 100, 200, 500], value=100)

    try:
        total_rows = get_table_rowcount(schema, table)
    except Exception as e:
        st.error(f"No pude contar filas de `{schema}.{table}`. Error: {e}")
        st.stop()

    total_pages = max((total_rows + page_size - 1) // page_size, 1)
    page = st.number_input("Página", min_value=1, max_value=total_pages, value=1, step=1)
    offset = int((page - 1) * page_size)

    st.divider()
    st.caption("Acciones")
    refresh = st.button("Refrescar caché (esta tabla)")


if refresh:
    load_table_sample.clear()
    get_table_rowcount.clear()


col_a, col_b = st.columns([2, 1], gap="large")

with col_a:
    st.subheader(f"Vista previa: `{schema}.{table}`")
    st.caption(f"Filas: {total_rows:,} · Página {page}/{total_pages}")

    with st.spinner("Cargando datos..."):
        df = load_table_sample(schema, table, limit=int(page_size), offset=int(offset))

    st.dataframe(df, use_container_width=True, height=520)

    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Descargar CSV (solo esta página)",
        data=csv,
        file_name=f"{schema}.{table}.page_{page}.csv",
        mime="text/csv",
        use_container_width=True,
    )

with col_b:
    st.subheader("Calidad rápida")
    st.metric("Columnas", int(df.shape[1]))
    st.metric("Filas (preview)", int(df.shape[0]))

    if not df.empty:
        st.caption("Top nulos (preview)")
        st.dataframe(nulls_profile(df).head(12), use_container_width=True, height=360)
    else:
        st.info("La vista previa está vacía.")


st.divider()

# Panel específico para Gold profesional (resumen_por_asesor)
if schema == "gold" and table == "resumen_por_asesor":
    st.subheader("KPIs & features (Gold) — `resumen_por_asesor`")

    # Cargar una muestra más grande para KPIs sin traer toda la tabla
    sample_limit = 5000
    with st.spinner("Calculando KPIs sobre una muestra..."):
        df_kpi = load_table_sample(schema, table, limit=sample_limit, offset=0)

    if df_kpi.empty:
        st.warning("No hay datos en `gold.resumen_por_asesor`.")
    else:
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Asesores (muestra)", int(df_kpi["id_asesor"].nunique()) if "id_asesor" in df_kpi.columns else 0)

        if "tmo_promedio" in df_kpi.columns:
            k2.metric("TMO promedio", float(pd.to_numeric(df_kpi["tmo_promedio"], errors="coerce").mean(skipna=True) or 0))
        else:
            k2.metric("TMO promedio", "—")

        if "score_calidad_promedio" in df_kpi.columns:
            k3.metric("Calidad promedio", float(pd.to_numeric(df_kpi["score_calidad_promedio"], errors="coerce").mean(skipna=True) or 0))
        else:
            k3.metric("Calidad promedio", "—")

        if "ratio_incidencias" in df_kpi.columns:
            k4.metric("Ratio incidencias (mean)", float(pd.to_numeric(df_kpi["ratio_incidencias"], errors="coerce").mean(skipna=True) or 0))
        else:
            k4.metric("Ratio incidencias (mean)", "—")

        if "riesgo" in df_kpi.columns:
            st.caption("Distribución de `riesgo` (muestra)")
            riesgo = pd.to_numeric(df_kpi["riesgo"], errors="coerce").fillna(0).astype(int)
            dist = riesgo.value_counts().rename_axis("riesgo").reset_index(name="asesores")
            st.bar_chart(dist.set_index("riesgo"))

        with st.expander("Diccionario rápido (columnas clave)", expanded=False):
            st.markdown(
                """
- **KPIs**: `total_llamadas`, `duracion_total`, `tmo_promedio`, `score_calidad_promedio`, `total_incidencias`, `adherencia_promedio`, `total_capacitaciones`
- **Features**: `ratio_incidencias`, `tmo_normalizado`, `calidad_normalizada`, `adherencia_normalizada`
- **Target**: `riesgo`
"""
            )