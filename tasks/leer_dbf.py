
# -------------------------------
# Tarea para leer archivos DBF
# -------------------------------
import os
from typing import Any, Dict, Iterable
from dbfread2 import DBF
from prefect import get_run_logger, task
from config.settings_config import settings

@task(retries=2, retry_delay_seconds=30)
def leer_dbf(path: str) -> Iterable[Dict[str, Any]]:
    logger = get_run_logger()
    logger.info(f"Iniciando lectura de DBF: {path}")
    full_path = os.path.join(settings.data_dir, path)
    if not os.path.exists(full_path):
        logger.error(f"Archivo DBF no encontrado: {full_path}")
        raise FileNotFoundError(f"DBF no encontrado: {full_path}")
    records = DBF(full_path, record_factory=dict, encoding="cp1252", char_decode_errors="replace")
    return records