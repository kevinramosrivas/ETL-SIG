import datetime
from config.tables_config import TABLES_QUERY,TableQuery
from typing import List
from config.models.extract_model import ExtraTableSettings 
from pathlib import Path
from prefect import variables
import yaml
import json


def get_years_to_extract(n=2):
    # Leer año actual desde variable, si no existe usar el año actual del sistema
    anio_actual_str = variables.get("anio_actual", default=str(datetime.datetime.now().now().year))
    anio_actual = int(anio_actual_str)

    # Leer cuántos años anteriores usar (opcional)
    anios_anteriores_str = variables.get("anios_anteriores", default="[]")
    try:
        anios_anteriores = json.loads(anios_anteriores_str)
        if not isinstance(anios_anteriores, list):
            raise ValueError()
        anios_anteriores = [int(y) for y in anios_anteriores]
    except Exception:
        # fallback: generar automáticamente años anteriores
        anios_anteriores = [anio_actual - i for i in range(1, n + 1)]

    return [str(y) for y in sorted(anios_anteriores + [anio_actual])]


def load_table_configs(path: str = "extract_config.yaml") -> ExtraTableSettings:
    full_path = Path(__file__).parent.parent / path
    with open(full_path, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)

    years = get_years_to_extract()
    print(get_years_to_extract())
    for table in raw_config.get("tables", []):
        filters = table.get("filters", {})
        for k, v in filters.items():
            if v == "__YEARS__":
                filters[k] = years

    return ExtraTableSettings(**raw_config)


def load_query_tables() -> List[TableQuery]:
    configs: List[TableQuery] = []
    for info in TABLES_QUERY:
        configs.append(
            TableQuery(
                table=info["table"],
                query= info["query"],
            )
        )
    return configs
