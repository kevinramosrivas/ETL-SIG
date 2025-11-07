from pydantic import BaseModel, Field, model_validator
from typing import List, Literal, Optional, Any

class ScraperConfig(BaseModel):
    module: str
    function: str
    args: List[Any] = Field(default_factory=list)

class TransformTableConfig(BaseModel):
    table: str
    partitioned: bool
    source_type: Literal["sql", "scraper"]
    table_type: Literal["dimension", "fact","dimension_base"] = "dimension"  # Nuevo campo opcional con valor por defecto
    # Campos opcionales según source_type
    query: Optional[str] = None
    scraper: Optional[ScraperConfig] = None

    @model_validator(mode="after")
    def check_source_deps(self) -> "TransformTableConfig":
        if self.source_type == "sql":
            if not self.query:
                raise ValueError("`query` es obligatorio cuando source_type='sql'")
            if self.scraper is not None:
                raise ValueError("`scraper` no debe usarse cuando source_type='sql'")
        else:  # source_type == "scraper"
            if not self.scraper:
                raise ValueError("`scraper` es obligatorio cuando source_type='scraper'")
            if self.query is not None:
                raise ValueError("`query` no debe usarse cuando source_type='scraper'")
        return self

class TransformTablesConfig(BaseModel):
    tables: List[TransformTableConfig]
