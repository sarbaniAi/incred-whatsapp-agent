"""Database layer using Databricks SQL connector for Unity Catalog tables."""

import logging
import os
import time
from contextlib import contextmanager
from threading import Lock

from databricks import sql as dbsql

logger = logging.getLogger(__name__)

_connection = None
_lock = Lock()


def _get_connection():
    """Get or create a Databricks SQL connection."""
    global _connection
    with _lock:
        if _connection is not None:
            try:
                _connection.cursor().execute("SELECT 1")
                return _connection
            except Exception:
                _connection = None

        host = os.environ.get("DATABRICKS_HOST", "")
        token = os.environ.get("DATABRICKS_TOKEN", "")
        warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

        if not host:
            # Inside Databricks App — use workspace client
            from databricks.sdk import WorkspaceClient
            wc = WorkspaceClient()
            host = wc.config.host.replace("https://", "")
            token = wc.config.token

            if not warehouse_id:
                # Find a running warehouse
                warehouses = list(wc.warehouses.list())
                for wh in warehouses:
                    if wh.state and wh.state.value == "RUNNING":
                        warehouse_id = wh.id
                        break
                if not warehouse_id and warehouses:
                    warehouse_id = warehouses[0].id

        if not warehouse_id:
            raise RuntimeError("DATABRICKS_WAREHOUSE_ID not set and no warehouse found")

        _connection = dbsql.connect(
            server_hostname=host,
            http_path=f"/sql/1.0/warehouses/{warehouse_id}",
            access_token=token,
        )
        logger.info(f"Connected to {host} warehouse {warehouse_id}")
        return _connection


def init_pool():
    """Initialize the connection (called at startup)."""
    _get_connection()


def execute(query: str, params: tuple = None) -> list[dict]:
    """Execute a query and return all rows as dicts."""
    conn = _get_connection()
    with conn.cursor() as cur:
        cur.execute(query, params)
        if cur.description:
            columns = [col[0] for col in cur.description]
            rows = cur.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        return []


def execute_one(query: str, params: tuple = None) -> dict | None:
    """Execute a query and return a single row."""
    rows = execute(query, params)
    return rows[0] if rows else None


def execute_write(query: str, params: tuple = None):
    """Execute an INSERT/UPDATE/DELETE."""
    conn = _get_connection()
    with conn.cursor() as cur:
        cur.execute(query, params)
