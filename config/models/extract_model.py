from typing import List, Optional, Dict, Union
from pydantic import BaseModel


class ExtractTableConfig(BaseModel):
    key: str
    source: str                      # archivo .dbf de origen
    target: str                      # tabla destino en PostgreSQL
    filters: Optional[Dict[str, Union[List[Union[str, int]], None]]] = None
    columns: List[str]
    field_map: Optional[Dict[str, str]] = None


class ExtraTableSettings(BaseModel):
    defaults: Optional[Dict[str, Union[bool, str, int]]] = {}
    tables: List[ExtractTableConfig]
