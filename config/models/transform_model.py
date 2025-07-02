from pydantic import BaseModel
from typing import List

class TransformTableConfig(BaseModel):
    table: str
    query: str
    partitioned: bool

class TransformTablesConfig(BaseModel):
    tables: List[TransformTableConfig]
