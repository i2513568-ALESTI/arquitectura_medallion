"""
Load environment variables and expose settings for the ETL pipeline.
Expects .env in the project root (parent of config/).
"""
import os
from pathlib import Path

from dotenv import load_dotenv

# Project root: parent of this package
PROJECT_ROOT = Path(__file__).resolve().parent.parent
_env_path = PROJECT_ROOT / ".env"

if _env_path.exists():
    load_dotenv(_env_path)
else:
    load_dotenv()

# Database (Supabase/PostgreSQL)
DB_HOST = os.getenv("DB_HOST", "aws-0-us-west-2.pooler.supabase.com")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres.zuxjvpqgdpprzostfiuf")
DB_PASSWORD = os.getenv("DB_PASSWORD", "xmatansa123")

# Schema and table names
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

BRONZE_TABLES = [
    "dim_asesor",
    "fact_reclutamiento",
    "fact_rendimiento_mensual",
    "fact_capacitacion",
    "fact_calidad",
    "fact_incidencias",
    "fact_adherencia",
    "fact_clima",
]


class _Settings:
    """Simple settings container."""

    def __init__(self):
        self.project_root = PROJECT_ROOT
        self.db_host = DB_HOST
        self.db_port = DB_PORT
        self.db_name = DB_NAME
        self.db_user = DB_USER
        self.db_password = DB_PASSWORD
        self.bronze_schema = BRONZE_SCHEMA
        self.silver_schema = SILVER_SCHEMA
        self.gold_schema = GOLD_SCHEMA
        self.bronze_tables = list(BRONZE_TABLES)

    def get_sqlalchemy_uri(self):
        """Build PostgreSQL URI for SQLAlchemy (password URL-encoded)."""
        from urllib.parse import quote_plus
        pwd = quote_plus(self.db_password)
        return (
            f"postgresql://{self.db_user}:{pwd}@{self.db_host}:{self.db_port}/{self.db_name}"
        )


settings = _Settings()
