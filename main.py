import os
import csv
import time
from io import StringIO
from typing import Any, Callable, Dict, Iterable, List, Optional
from utils.load_tables_config import load_table_configs
from utils.filter import build_filter
from dbfread2 import DBF
import psycopg2
import psycopg2.extras
from pydantic_settings import BaseSettings
from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE


class Settings(BaseSettings):
    db_name: str
    db_user: str
    db_password: str
    db_host: str = "localhost"
    db_port: int = 5432
    data_dir: str = "DATA"

    class Config:
        env_file = ".env"


settings = Settings()



# Deshabilitar cache devolviendo siempre None en cache_key_fn
@task(retries=2, retry_delay_seconds=30, cache_policy=NO_CACHE)
def read_dbf(path: str) -> Iterable[Dict[str, Any]]:
    logger = get_run_logger()
    full_path = os.path.join(settings.data_dir, path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"DBF file not found: {full_path}")
    logger.info(f"Reading DBF: {full_path}")
    return DBF(full_path, record_factory=dict, encoding="cp1252", char_decode_errors="replace")



# Deshabilitar cache para la conexión
@task(retries=3, retry_delay_seconds = 30 , cache_policy=NO_CACHE)
def connect_db() -> psycopg2.extras.DictCursor:
    conn = psycopg2.connect(
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        host=settings.db_host,
        port=settings.db_port,
    )
    conn.autocommit = True
    return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)



# Deshabilitar cache en bulk_load
@task(retries=1, retry_delay_seconds=10,cache_policy=NO_CACHE)
def bulk_load(
    cursor: psycopg2.extras.DictCursor,
    table: str,
    columns: List[str],
    records: Iterable[Dict[str, Any]],
    field_map: Optional[Dict[str, str]] = None,
    filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
    truncate: bool = False,
):
    logger = get_run_logger()
    start = time.time()

    if truncate:
        cursor.execute(f"TRUNCATE {table} RESTART IDENTITY CASCADE;")
        cursor.execute(f"ALTER TABLE {table} DISABLE TRIGGER ALL;")

    buf = StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(columns)

    count = 0
    for rec in records:
        if filter_fn and not filter_fn(rec):
            continue
        row = [rec.get(field_map.get(col, col)) for col in columns]
        writer.writerow(row)
        count += 1
    buf.seek(0)

    cols_sql = ", ".join(columns)
    cursor.copy_expert(f"COPY {table} ({cols_sql}) FROM STDIN WITH (FORMAT csv, HEADER TRUE)", buf)

    if truncate:
        cursor.execute(f"ALTER TABLE {table} ENABLE TRIGGER ALL;")

    logger.info(
        f"Loaded {count} rows into {table} in {round(time.time() - start, 2)}s"
    )
    return True


@flow
def etl_sig():
    cursor = connect_db()
    tables = load_table_configs()
    for cfg in tables:
        data = read_dbf(cfg.path)
        filter_fn = build_filter(cfg.filter_fields) if cfg.filter_fields else None
        bulk_load(
            cursor=cursor,
            table=cfg.table,
            columns=cfg.columns,
            records=data,
            field_map=cfg.field_map,
            filter_fn=filter_fn,
            truncate=cfg.truncate,
        )

if __name__ == "__main__":
    etl_sig()
