import datetime
from typing import Any, List

from pydantic import ValidationError
from config.models.extract_model import ExtraTableSettings 
from config.models.transform_model import TransformTablesConfig
from pathlib import Path
import yaml

def _replace_placeholder(obj: Any, placeholder: str, value: str) -> Any:
    """
    Recursivamente reemplaza placeholder en cualquier str dentro de obj.
    - Si es str, lo reemplaza.
    - Si es list, recorre cada elemento.
    - Si es dict, recorre cada clave/valor.
    - En otro caso, lo devuelve tal cual.
    """
    if isinstance(obj, str):
        return obj.replace(placeholder, value)
    elif isinstance(obj, list):
        return [_replace_placeholder(v, placeholder, value) for v in obj]
    elif isinstance(obj, dict):
        return {
            k: _replace_placeholder(v, placeholder, value)
            for k, v in obj.items()
        }
    else:
        return obj
    


def get_years_to_extract(n=1):
    # Leer año actual desde variable, si no existe usar el año actual del sistema
    periodo_hasta = int(datetime.datetime.now().now().year)
    periodo_desde = int(datetime.datetime.now().now().year - n) 
    anios = []
    if(periodo_desde>= periodo_hasta):
        raise Exception("el periodo desde no puede ser mayor al periodo hasta")
    for year in range(periodo_desde, periodo_hasta + 1):
        anios.append(str(year))
    return anios

## Carga la configuración de las tablas de dimensión
def load_table_configs(path: str = "extract_config.yaml") -> ExtraTableSettings:
    full_path = Path(__file__).parent.parent / path
    with open(full_path, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)

    years = get_years_to_extract()
    for table in raw_config.get("tables", []):        
        filters = table.get("filters", {})
        # Reemplaza __YEARS__ por la lista de años
        for k, v in filters.items():
            if v == "__YEARS__":
                filters[k] = years

    return ExtraTableSettings(**raw_config)



def load_table_tranform(year: str, path: str = "transform_config.yaml",table_type: str = "dimension") -> TransformTablesConfig:
    """
    Carga la configuración de transformación desde un YAML, reemplaza
    cualquier ocurrencia de __ANIO_EJECUCION__ con `year` en todo el dict,
    y valida contra el modelo Pydantic.
    Args:
        year: año de ejecución para reemplazar en la configuración.
        table_type: tipo de tabla a filtrar (por defecto "dimension").
    Returns:
        TransformTablesConfig: objeto con la configuración de transformación.
    Raises:
        RuntimeError: si hay un error de validación en el YAML.
    Raises:
        FileNotFoundError: si el archivo de configuración no existe.
    Raises:
        ValidationError: si la configuración no cumple con el modelo Pydantic.
    """
    full_path = Path(__file__).parent.parent / path

    with open(full_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    # Aplica el reemplazo recursivo en todo el dict
    raw_replaced = _replace_placeholder(raw, "__ANIO_EJECUCION__", year)

    try:
        config = TransformTablesConfig(**raw_replaced)
        # Filtra las tablas por tipo
        config.tables = [t for t in config.tables if t.table_type == table_type]
    except ValidationError as exc:
        raise RuntimeError(f"Error validando transform_config.yaml:\n{exc}") from exc

    return config
    
