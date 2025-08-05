from pydantic import BaseModel
from typing import List

class VwMaterializedSettings(BaseModel):
    schema_name: str
    tables: List[str]
