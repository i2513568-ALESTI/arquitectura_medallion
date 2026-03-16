# Y04AN1_BDA_P1

ETL pipeline for Supabase (PostgreSQL) using **Medallion architecture**: Bronze → Silver → Gold.

## Setup

1. **Environment**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate   # Windows
   pip install -r requirements.txt
   ```

2. **Config**
   - Copy `.env.example` to `.env` and set your Supabase credentials (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`).

## Run pipeline

From project root:

```bash
python run_pipeline.py
```

- Loads tables from **bronze** schema.
- Cleans data into **silver** (dedup, trim strings, nulls).
- Builds **gold** (aggregations by `id_asesor`).
- Creates **silver** and **gold** schemas if missing.
- Writes results with `pandas.to_sql`.

## Layout

- `config/` – settings and DB (env, `get_connection`, `get_engine`).
- `etl/` – `bronze` (load), `silver` (clean), `gold` (aggregate), `pipeline` (orchestrate).
- `run_pipeline.py` – entry point with logging.

## Notebooks

- `notebook/carga_bronze.ipynb` – explore bronze.
- `notebook/carga_silver_gold.ipynb` – run Silver/Gold in Jupyter.
