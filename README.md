# SATDPO (Y04AN1_BDA_P1)

Pipeline de datos para Supabase (PostgreSQL) usando **arquitectura Medallion**: **Bronze → Silver → Gold**.

- **Silver**: limpieza por dominio (tipos, nulos críticos, validaciones, normalización de columnas).
- **Gold**: dataset consolidado por `id_asesor` con **KPIs**, **features para ML** y `riesgo` (target) para modelado.
- **Streamlit**: visor profesional para explorar tablas **silver** y **gold**.

## Requisitos

- Python 3.10+ (recomendado 3.12)
- Acceso a una base PostgreSQL/Supabase con schema `bronze` poblado

## Configuración (sin secretos)

1. Copia `.env.example` a `.env`.
2. Completa tus credenciales de Supabase/PostgreSQL:
   - `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

Importante:
- **No subas `.env` a GitHub**. Este repo ya lo ignora en `.gitignore`.

## Instalación local

```bash
python -m venv .venv
.venv\Scripts\activate   # Windows
pip install -r requirements.txt
```

## Ejecutar el pipeline (ETL)

Desde la raíz del proyecto:

```bash
python run_pipeline.py
```

Qué hace:
- **Lee Bronze** desde `bronze.*`.
- **Construye Silver** (limpieza y tipado).
- **Construye Gold** (`gold.resumen_por_asesor`) con KPIs, features y `riesgo`.
- Crea schemas `silver` y `gold` si no existen.
- Escribe resultados en Supabase/PostgreSQL.

## Ejecutar el dashboard (Streamlit)

```bash
streamlit run app.py
```

Incluye:
- selector de schema (`silver`/`gold`) y tabla
- vista previa paginada
- perfil rápido de nulos
- panel especial para `gold.resumen_por_asesor` (KPIs y distribución de `riesgo`)

## Deploy (Streamlit Community Cloud)

1. Sube este repo a GitHub (sin `.env`).
2. En Streamlit Cloud, configura:
   - **Main file path**: `app.py`
3. En **App settings → Secrets**, agrega tus variables:

```toml
DB_HOST="..."
DB_PORT="5432"
DB_NAME="postgres"
DB_USER="..."
DB_PASSWORD="..."
```

## Estructura del repo

- `config/`: settings y conexión (`get_connection`, `get_engine`).
- `etl/`: capas Bronze/Silver/Gold y orquestación.
- `run_pipeline.py`: entrypoint del ETL.
- `app.py`: dashboard Streamlit.
- `notebook/`: notebooks de exploración/ejecución.
