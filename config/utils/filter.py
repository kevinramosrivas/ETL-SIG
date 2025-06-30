from typing import Any, Callable, Dict, Iterable, List, Optional
def build_filter(fields: Dict[str, Optional[List[Any]]]) -> Callable[[Dict[str, Any]], bool]:
    def _filter(rec: Dict[str, Any]) -> bool:
        for key, valid in fields.items():
            if key not in rec or rec.get(key) is None:
                return False
            if valid is not None and rec[key] not in valid:
                return False
        return True

    return _filter