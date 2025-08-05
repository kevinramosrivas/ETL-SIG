from pathlib import Path
import yaml
from config.models.vw_materialized_model import VwMaterializedSettings

def load_views_configs(path: str = "vw_materialized_config.yaml") -> VwMaterializedSettings:
    full_path = Path(__file__).parent.parent / path
    with open(full_path, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)
    return VwMaterializedSettings(**raw_config)