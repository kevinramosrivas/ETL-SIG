from pydantic_settings import BaseSettings
from typing import Any, Callable, Dict, Iterable, List, Optional
class TableConfig(BaseSettings):
    key:str
    table: str
    path: str
    columns: List[str]
    field_map: Optional[Dict[str, str]] = None
    filter_fields: Optional[Dict[str, Optional[List[Any]]]] = None
    truncate: bool = True

# Diccionario maestro de columnas por clave lógica
BASE_COLUMNS = {
    "certificado": [
        "ANO_EJE", "CERTIFICADO", "SEC_EJEC", "TIPO_CERTIFICADO",
        "ESTADO_REGISTRO", "COD_ERROR", "COD_MENSA", "ESTADO_ENVIO",
    ],
    "certificado_fase": [
        "ANO_EJE", "SEC_EJEC", "CERTIFICADO", "SECUENCIA", "SECUENCIA_PADRE",
        "FUENTE_FINANC", "ETAPA", "TIPO_ID", "RUC", "ES_COMPROMISO",
        "MONTO", "MONTO_COMPROMETIDO", "MONTO_NACIONAL", "GLOSA",
        "ESTADO_REGISTRO", "COD_ERROR", "COD_MENSA", "ESTADO_ENVIO",
        "SALDO_NACIONAL", "IND_ANULACION", "TIPO_FINANCIAMIENTO",
        "TIPO_OPERACION", "SEC_AREA",
    ],
    "certificado_secuencia": [
        "ANO_EJE", "SEC_EJEC", "CERTIFICADO", "SECUENCIA", "CORRELATIVO",
        "COD_DOC", "NUM_DOC", "FECHA_DOC", "ESTADO_REGISTRO", "ESTADO_ENVIO",
        "IND_CERTIFICACION", "ESTADO_REGISTRO2", "ESTADO_ENVIO2", "MONTO",
        "MONTO_COMPROMETIDO", "MONTO_NACIONAL", "MONEDA", "TIPO_CAMBIO",
        "COD_ERROR", "COD_MENSA", "TIPO_REGISTRO", "FECHA_BD_ORACLE",
        "ESTADO_CTB", "SECUENCIA_SOLICITUD", "FECHA_CREACION_CLT",
        "FECHA_MODIFICACION_CLT", "FLG_INTERFASE",
    ],
    "certificado_meta": [
        "ANO_EJE", "SEC_EJEC", "CERTIFICADO", "SECUENCIA", "CORRELATIVO",
        "ID_CLASIFICADOR", "SEC_FUNC", "MONTO", "MONTO_COMPROMETIDO",
        "MONTO_NACIONAL", "ESTADO_REGISTRO", "COD_ERROR", "COD_MENSA",
        "ESTADO_ENVIO", "MONTO_NACIONAL_AJUSTE", "SYS_COD_CLASIF",
        "SYS_ID_CLASIFICADOR",
    ],
    "expediente_fase": [
        "ANO_EJE", "SEC_EJEC", "EXPEDIENTE", "CICLO", "FASE", "SECUENCIA",
        "SECUENCIA_PADRE", "SECUENCIA_ANTERIOR", "MES_CTB", "MONTO_NACIONAL",
        "MONTO_SALDO", "ORIGEN", "FUENTE_FINANC", "MEJOR_FECHA", "TIPO_ID",
        "RUC", "TIPO_PAGO", "TIPO_RECURSO", "TIPO_COMPROMISO", "ORGANISMO",
        "PROYECTO", "ESTADO", "ESTADO_ENVIO", "ARCHIVO", "TIPO_GIRO",
        "TIPO_FINANCIAMIENTO", "COD_DOC_REF", "FECHA_DOC_REF", "NUM_DOC_REF",
        "CERTIFICADO", "CERTIFICADO_SECUENCIA", "SEC_EJEC_RUC",
    ],
}

# Diccionario maestro de mapeos de campo por clave
BASE_FIELD_MAP = {
    "certificado": {
        "CERTIFICADO": "CERTIFICAD",
        "TIPO_CERTIFICADO": "TIPO_CERTI",
        "ESTADO_REGISTRO": "ESTADO_REG",
        "ESTADO_ENVIO": "ESTADO_ENV",
    },
    "certificado_fase": {
        "CERTIFICADO": "CERTIFICAD",
        "SECUENCIA_PADRE": "SECUENCIA_",
        "FUENTE_FINANC": "FUENTE_FIN",
        "ES_COMPROMISO": "ES_COMPROM",
        "MONTO_COMPROMETIDO": "MONTO_COMP",
        "MONTO_NACIONAL": "MONTO_NACI",
        "ESTADO_REGISTRO": "ESTADO_REG",
        "ESTADO_ENVIO": "ESTADO_ENV",
        "SALDO_NACIONAL": "SALDO_NACI",
        "IND_ANULACION": "IND_ANULAC",
        "TIPO_FINANCIAMIENTO": "TIPO_FINAN",
        "TIPO_OPERACION": "TIPO_OPERA",
    },
    "certificado_secuencia": {
        "CERTIFICADO": "CERTIFICAD",
        "CORRELATIVO": "CORRELATIV",
        "MONTO_COMPROMETIDO": "MONTO_COMP",
        "MONTO_NACIONAL": "MONTO_NACI",
        "ESTADO_REGISTRO": "ESTADO_REG",
        "ESTADO_ENVIO": "ESTADO_ENV",
        "IND_CERTIFICACION": "IND_CERTIF",
        "TIPO_CAMBIO": "TIPO_CAMBI",
        "TIPO_REGISTRO": "TIPO_REGIS",
        "FECHA_BD_ORACLE": "FECHA_BD_O",
        "FECHA_CREACION_CLT": "FECHA_CREA",
        "FECHA_MODIFICACION_CLT": "FECHA_MODI",
    },
    "certificado_meta": {
        "CERTIFICADO": "CERTIFICAD",
        "CORRELATIVO": "CORRELATIV",
        "MONTO_COMPROMETIDO": "MONTO_COMP",
        "MONTO_NACIONAL": "MONTO_NACI",
        "ESTADO_REGISTRO": "ESTADO_REG",
        "ESTADO_ENVIO": "ESTADO_ENV",
        "ID_CLASIFICADOR": "ID_CLASIFI",
        "MONTO_NACIONAL_AJUSTE": "MONTO_NAC2",
    },
    "expediente_fase": {
        "SECUENCIA_PADRE": "SECUENCIA2",
        "SECUENCIA_ANTERIOR": "SECUENCIA_",
        "MONTO_NACIONAL": "MONTO_NACI",
        "MONTO_SALDO": "MONTO_SALD",
        "FUENTE_FINANC": "FUENTE_FIN",
        "MEJOR_FECHA": "MEJOR_FECH",
        "TIPO_RECURSO": "TIPO_RECUR",
        "TIPO_COMPROMISO": "TIPO_COMPR",
        "TIPO_FINANCIAMIENTO": "TIPO_FINAN",
        "COD_DOC_REF": "COD_DOC_RE",
        "FECHA_DOC_REF": "FECHA_DOC_",
        "NUM_DOC_REF": "NUM_DOC_RE",
        "CERTIFICADO": "CERTIFICAD",
        "CERTIFICADO_SECUENCIA": "CERTIFICA2",
        "SEC_EJEC_RUC": "SEC_EJEC_R",
    },
}

# Lista mínima de metadatos para generar configs de Prefect
TABLES_INFO = [
    {
        "key": "certificado",
        "table": "bytsscom_bytsiaf.certificado",
        "path": "certificado.dbf",
        "filter_fields": {
            "CERTIFICAD": None,
            "ANO_EJE": ["2024", "2025"],
            "ESTADO_REG": ["A"],
            "TIPO_CERTI": ["2"],
        },
    },
    {
        "key": "certificado_fase",
        "table": "bytsscom_bytsiaf.certificado_fase",
        "path": "certificado_fase.dbf",
        "filter_fields": {
            "CERTIFICAD": None,
            "ANO_EJE": ["2024", "2025"],
        },
    },
    {
        "key": "certificado_secuencia",
        "table": "bytsscom_bytsiaf.certificado_secuencia",
        "path": "certificado_secuencia.dbf",
        "filter_fields": {
            "CERTIFICAD": None,
            "ANO_EJE": ["2024", "2025"],
        },
    },
    {
        "key": "certificado_meta",
        "table": "bytsscom_bytsiaf.certificado_meta",
        "path": "certificado_meta.dbf",
        "filter_fields": {
            "CERTIFICAD": None,
            "ANO_EJE": ["2024", "2025"],
        },
    },
    {
        "key": "expediente_fase",
        "table": "bytsscom_bytsiaf.expediente_fase",
        "path": "expediente_fase.dbf",
        "filter_fields": {
            "CERTIFICAD": None,
            "ANO_EJE": ["2024", "2025"],
        },
    },
]
