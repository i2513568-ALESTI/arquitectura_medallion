"""
Database connections: raw psycopg2 and SQLAlchemy engine for pandas.
"""
import logging

import psycopg2
from sqlalchemy import create_engine

from config.settings import settings

logger = logging.getLogger(__name__)


def get_connection():
    """Return a psycopg2 connection to the database."""
    try:
        conn = psycopg2.connect(
            host=settings.db_host,
            port=settings.db_port,
            database=settings.db_name,
            user=settings.db_user,
            password=settings.db_password,
        )
        logger.debug("Database connection established (psycopg2)")
        return conn
    except Exception as e:
        logger.exception("Failed to connect to database: %s", e)
        raise


def get_engine():
    """Return a SQLAlchemy engine for pandas.to_sql."""
    try:
        engine = create_engine(settings.get_sqlalchemy_uri())
        logger.debug("SQLAlchemy engine created")
        return engine
    except Exception as e:
        logger.exception("Failed to create SQLAlchemy engine: %s", e)
        raise
