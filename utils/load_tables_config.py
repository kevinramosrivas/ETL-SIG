
from config.tables_config import BASE_COLUMNS, BASE_FIELD_MAP, TABLES_INFO,TABLES_QUERY,TableConfig,TableQuery
from typing import List
def load_table_configs() -> List[TableConfig]:
    configs: List[TableConfig] = []
    for info in TABLES_INFO:
        key = info["key"]
        configs.append(
            TableConfig(
                key= info["key"],
                table=info["table"],
                path=info["path"],
                columns=BASE_COLUMNS[key],
                field_map=BASE_FIELD_MAP.get(key),
                filter_fields=info.get("filter_fields"),
                truncate=True,
            )
        )
    return configs

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
