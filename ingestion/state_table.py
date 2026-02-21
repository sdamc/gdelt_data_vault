"""State table management for GDELT ingestion tracking.

This module provides utilities to create and interact with metadata tables
that track ingestion runs, file processing status, and watermarks.
"""

from typing import Optional
from datetime import datetime
import os
import psycopg2
from psycopg2.extensions import connection as PGConnection


# SQL DDL for state tables
CREATE_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS meta;
"""

CREATE_WATERMARK_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS meta.ingestion_watermark (
  dataset TEXT PRIMARY KEY,
  last_successful_file_ts BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_run_id TEXT
);
"""

CREATE_RUNS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS meta.ingestion_runs (
  run_id TEXT PRIMARY KEY,
  dataset TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at TIMESTAMPTZ,
  status TEXT NOT NULL DEFAULT 'running', -- running|success|partial|failed
  max_files_per_run INT NOT NULL,
  catalog_snapshot_at TIMESTAMPTZ,
  files_detected INT DEFAULT 0,
  files_selected INT DEFAULT 0,
  files_downloaded INT DEFAULT 0,
  files_failed INT DEFAULT 0,
  error_message TEXT
);
"""

CREATE_FILES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS meta.ingestion_files (
  dataset TEXT NOT NULL,
  file_ts BIGINT NOT NULL,
  url TEXT NOT NULL,
  filename TEXT NOT NULL,
  run_id TEXT,
  status TEXT NOT NULL DEFAULT 'queued', -- queued|downloading|downloaded|skipped|failed
  bytes BIGINT,
  checksum TEXT,
  download_started_at TIMESTAMPTZ,
  download_ended_at TIMESTAMPTZ,
  error_message TEXT,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (dataset, file_ts)
);
"""

CREATE_INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS ix_ingestion_files_run ON meta.ingestion_files(run_id);
CREATE INDEX IF NOT EXISTS ix_ingestion_files_status ON meta.ingestion_files(status);
"""


def create_state_tables(conn: PGConnection) -> None:
    """Create all state tables and indexes.
    
    Args:
        conn: PostgreSQL database connection
    """
    with conn.cursor() as cur:
        cur.execute(CREATE_SCHEMA_SQL)
        cur.execute(CREATE_WATERMARK_TABLE_SQL)
        cur.execute(CREATE_RUNS_TABLE_SQL)
        cur.execute(CREATE_FILES_TABLE_SQL)
        cur.execute(CREATE_INDEXES_SQL)
    conn.commit()


def get_connection(
    host: str = None,
    port: int = None,
    database: str = None,
    user: str = None,
    password: str = None
) -> PGConnection:
    """Get a PostgreSQL database connection.
    
    Defaults are read from environment variables if not provided:
    - POSTGRES_HOST (default: 'postgres')
    - POSTGRES_PORT (default: 5432)
    - POSTGRES_DB (default: 'gdelt')
    - POSTGRES_USER (default: 'airflow')
    - POSTGRES_PASSWORD (default: 'airflow')
    
    Args:
        host: Database host
        port: Database port
        database: Database name
        user: Database user
        password: Database password
        
    Returns:
        PostgreSQL connection object
    """
    return psycopg2.connect(
        host=host or os.getenv('POSTGRES_HOST', 'postgres'),
        port=port or int(os.getenv('POSTGRES_PORT', '5432')),
        database=database or os.getenv('POSTGRES_DB', 'gdelt'),
        user=user or os.getenv('POSTGRES_USER', 'airflow'),
        password=password or os.getenv('POSTGRES_PASSWORD', 'airflow')
    )


def init_run(
    conn: PGConnection,
    run_id: str,
    dataset: str,
    max_files_per_run: int
) -> None:
    """Initialize a new ingestion run.
    
    Args:
        conn: PostgreSQL database connection
        run_id: Unique identifier for the run
        dataset: Dataset name (e.g., 'gkg')
        max_files_per_run: Maximum number of files to process in this run
    """
    sql = """
    INSERT INTO meta.ingestion_runs (run_id, dataset, max_files_per_run, catalog_snapshot_at)
    VALUES (%s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, dataset, max_files_per_run, datetime.now()))
    conn.commit()


def update_run_status(
    conn: PGConnection,
    run_id: str,
    status: str,
    files_detected: Optional[int] = None,
    files_selected: Optional[int] = None,
    files_downloaded: Optional[int] = None,
    files_failed: Optional[int] = None,
    error_message: Optional[str] = None
) -> None:
    """Update the status of an ingestion run.
    
    Args:
        conn: PostgreSQL database connection
        run_id: Unique identifier for the run
        status: Status value (running|success|partial|failed)
        files_detected: Number of files detected in catalog
        files_selected: Number of files selected for download
        files_downloaded: Number of files successfully downloaded
        files_failed: Number of files that failed to download
        error_message: Error message if status is failed
    """
    updates = ["status = %s"]
    params = [status]
    
    if files_detected is not None:
        updates.append("files_detected = %s")
        params.append(files_detected)
    
    if files_selected is not None:
        updates.append("files_selected = %s")
        params.append(files_selected)
    
    if files_downloaded is not None:
        updates.append("files_downloaded = %s")
        params.append(files_downloaded)
    
    if files_failed is not None:
        updates.append("files_failed = %s")
        params.append(files_failed)
    
    if error_message is not None:
        updates.append("error_message = %s")
        params.append(error_message)
    
    if status in ('success', 'partial', 'failed'):
        updates.append("ended_at = %s")
        params.append(datetime.now())
    
    params.append(run_id)
    
    sql = f"""
    UPDATE meta.ingestion_runs
    SET {', '.join(updates)}
    WHERE run_id = %s
    """
    
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()


def insert_file_record(
    conn: PGConnection,
    dataset: str,
    file_ts: int,
    url: str,
    filename: str,
    run_id: str,
    status: str = 'queued'
) -> None:
    """Insert or update a file record.
    
    Args:
        conn: PostgreSQL database connection
        dataset: Dataset name
        file_ts: File timestamp (YYYYMMDDHHMISS as integer)
        url: Download URL
        filename: Filename
        run_id: Associated run ID
        status: File status (queued|downloading|downloaded|skipped|failed)
    """
    sql = """
    INSERT INTO meta.ingestion_files 
        (dataset, file_ts, url, filename, run_id, status)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (dataset, file_ts) 
    DO UPDATE SET 
        run_id = EXCLUDED.run_id,
        status = EXCLUDED.status,
        updated_at = now()
    """
    
    with conn.cursor() as cur:
        cur.execute(sql, (dataset, file_ts, url, filename, run_id, status))
    conn.commit()


def update_file_status(
    conn: PGConnection,
    dataset: str,
    file_ts: int,
    status: str,
    bytes_downloaded: Optional[int] = None,
    checksum: Optional[str] = None,
    error_message: Optional[str] = None
) -> None:
    """Update the status of a file.
    
    Args:
        conn: PostgreSQL database connection
        dataset: Dataset name
        file_ts: File timestamp
        status: New status (queued|downloading|downloaded|skipped|failed)
        bytes_downloaded: Size of downloaded file in bytes
        checksum: File checksum (e.g., MD5)
        error_message: Error message if status is failed
    """
    updates = ["status = %s", "updated_at = now()"]
    params = [status]
    
    if status == 'downloading':
        updates.append("download_started_at = %s")
        params.append(datetime.now())
    
    if status in ('downloaded', 'failed'):
        updates.append("download_ended_at = %s")
        params.append(datetime.now())
    
    if bytes_downloaded is not None:
        updates.append("bytes = %s")
        params.append(bytes_downloaded)
    
    if checksum is not None:
        updates.append("checksum = %s")
        params.append(checksum)
    
    if error_message is not None:
        updates.append("error_message = %s")
        params.append(error_message)
    
    params.extend([dataset, file_ts])
    
    sql = f"""
    UPDATE meta.ingestion_files
    SET {', '.join(updates)}
    WHERE dataset = %s AND file_ts = %s
    """
    
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()


def get_last_watermark(conn: PGConnection, dataset: str) -> int:
    """Get the last successful file timestamp for a dataset.
    
    Args:
        conn: PostgreSQL database connection
        dataset: Dataset name
        
    Returns:
        Last successful file timestamp, or 0 if no watermark exists
    """
    sql = """
    SELECT last_successful_file_ts 
    FROM meta.ingestion_watermark 
    WHERE dataset = %s
    """
    
    with conn.cursor() as cur:
        cur.execute(sql, (dataset,))
        row = cur.fetchone()
        return row[0] if row else 0


def update_watermark(
    conn: PGConnection,
    dataset: str,
    last_successful_file_ts: int,
    run_id: str
) -> None:
    """Update the watermark for a dataset.
    
    Args:
        conn: PostgreSQL database connection
        dataset: Dataset name
        last_successful_file_ts: Last successfully processed file timestamp
        run_id: Run ID that achieved this watermark
    """
    sql = """
    INSERT INTO meta.ingestion_watermark (dataset, last_successful_file_ts, last_run_id, updated_at)
    VALUES (%s, %s, %s, now())
    ON CONFLICT (dataset) 
    DO UPDATE SET 
        last_successful_file_ts = EXCLUDED.last_successful_file_ts,
        last_run_id = EXCLUDED.last_run_id,
        updated_at = now()
    """
    
    with conn.cursor() as cur:
        cur.execute(sql, (dataset, last_successful_file_ts, run_id))
    conn.commit()


def get_files_by_status(conn: PGConnection, run_id: str, status: str) -> list:
    """Get files for a run by status.
    
    Args:
        conn: PostgreSQL database connection
        run_id: Run ID
        status: File status to filter by
        
    Returns:
        List of file records
    """
    sql = """
    SELECT dataset, file_ts, url, filename, status, bytes, checksum, error_message
    FROM meta.ingestion_files
    WHERE run_id = %s AND status = %s
    ORDER BY file_ts
    """
    
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, status))
        return cur.fetchall()
