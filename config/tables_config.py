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

class TableQuery(BaseSettings):
    table: str
    query:str

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


TABLES_QUERY = queries = [
    {
        "table": "sistema_informacion_gerencial.dm_area",
        "query": """
        select
            id_area,
            area_name,
            cod_siaf_area,
            area_display_name,
            2 as nivel,
            case id_parent_area when 10468 then 10468
                else 11327 end as idsuperior
        from bytsscom_bytcore.area
        where length(area_name) = 3 and cod_siaf_area not in ('0000', '0001')
        union
        select
            id_area,
            area_name,
            cod_siaf_area,
            area_display_name,
            2 as nivel,
            case id_parent_area when 10468 then 10468
                else 11327 end as is_central
        from bytsscom_bytcore.area
        where id_parent_area = 10030 and length(area_name) = 5
        and id_area in (10383, 10390, 10395, 10385)
        union
        select
            10000 as id_area,
            '000' as area_name,
            '0000' as cod_siaf_area,
            'UNMSM' as area_display_name,
               0 as nivel,
               0 as idsuperior
        union
        select
            11327 as id_area,
            'D65' as area_name,
            '0001' as cod_siaf_area,
            'ADMINISTRACION CENTRAL' as area_display_name,
            1 as nivel,
            10000 as idsuperior
        union
        select
            10468 as id_area,
            'F' as area_name,
            '1000' as cod_siaf_area,
            'FACULTADES' as area_display_name,
            1 as nivel,
            10000 as idsuperior;
        """
    },
        {
            "table": "sistema_informacion_gerencial.dm_fuente",
            "query": """
    select
        siaf_codigo as fuente_siaf,
        desc_fuente
    from bytsscom_bytcore.fuente
    where id_fuente < 4
    order by siaf_codigo;
    """
        },
        {
            "table": "sistema_informacion_gerencial.dm_generica",
            "query": """
    select
        id_clasificador as id_generica,
        cod_clasif as cod_generica,
        nomb_clasif as nomb_generica
    from bytsscom_bytcore.clasificador
    where id_nivel = 2;
    """
        },
        {
            "table": "sistema_informacion_gerencial.hechos_institucional_consolidados",
            "query": """
    with certificado_anio_area as (
        select distinct
            c.siaf_certificado,
            cert.ano_eje,
            a.cod_siaf_area
        from bytsscom_bytsig.vw_certificacion c
        inner join bytsscom_bytsig.memo_requerimiento m
            on c.id_memo_requerimiento = m.id_memo_requerimiento and c.id_anio = m.id_anio
        inner join bytsscom_bytsiaf.certificado cert
            on c.id_anio::varchar = cert.ano_eje and c.siaf_certificado = cert.certificado
        inner join bytsscom_bytcore.area a
            on a.id_area = m.id_area
        where c.esta_cert = 'A' and tipo_cert = 'DP'
    )
    select
        cf.ano_eje as anio,
        certificado as num_certificado,
        coalesce(caa.cod_siaf_area, '0000') as cod_siaf_area,
        TRIM(fuente_financ) as fuente_siaf,
        monto
    from bytsscom_bytsiaf.certificado_fase cf
    left join certificado_anio_area caa
        on caa.siaf_certificado = cf.certificado and caa.ano_eje = cf.ano_eje
    where es_compromiso = 'N' and estado_registro = 'A' and secuencia = '0001'
    group by cf.ano_eje, certificado, monto, fuente_financ, caa.cod_siaf_area;
    """
        },
        {
            "table": "sistema_informacion_gerencial.dm_certificado",
            "query": """
    with certificado_anio_area as (
        select distinct
            c.siaf_certificado,
            cert.ano_eje,
            a.cod_siaf_area
        from bytsscom_bytsig.vw_certificacion c
        inner join bytsscom_bytsig.memo_requerimiento m
            on c.id_memo_requerimiento = m.id_memo_requerimiento and c.id_anio = m.id_anio
        inner join bytsscom_bytsiaf.certificado cert
            on c.id_anio::varchar = cert.ano_eje and c.siaf_certificado = cert.certificado
        inner join bytsscom_bytcore.area a
            on a.id_area = m.id_area
        where c.esta_cert = 'A' and tipo_cert = 'DP'
    )
    select
        cert.ano_eje as ano_eje,
        cert.certificado as num_certificado,
        cf.secuencia,
        cf.glosa,
        f.siaf_codigo as siaf_id_fuente,
        f.abre_fuente as fuente,
        cs.cod_doc,
        cs.num_doc,
        cs.estado_envio,
        cs.estado_registro,
        cs.fecha_creacion_clt,
        cla.codigo_siaf as siaf_id_clasificador,
        cla.cod_clasif as clasificador,
        substr(cla.cod_clasif, 0, 3) as generica,
        mis.id_meta_institucional as idmeta,
        mis.sec_func as codmeta,
        mp.nomb_met_ins,
        coalesce(areas.cod_siaf_area, '0000') as cod_siaf_area,
        sum(cs.monto_nacional) as monto_nacional,
        sum(cm.monto_nacional) as monto_clasificador
    from bytsscom_bytsiaf.certificado cert
    inner join bytsscom_bytsiaf.certificado_fase cf
        on cert.certificado = cf.certificado and cert.ano_eje = cf.ano_eje
    inner join bytsscom_bytcore.fuente f
        on cf.fuente_financ = f.siaf_codigo
    inner join bytsscom_bytsiaf.certificado_secuencia cs
        on cert.certificado = cs.certificado and cert.ano_eje = cs.ano_eje and cf.secuencia = cs.secuencia and cs.estado_registro = 'A'
    inner join bytsscom_bytsiaf.certificado_meta cm
        on cf.certificado = cm.certificado and cf.ano_eje = cm.ano_eje and cf.secuencia = cm.secuencia
    inner join bytsscom_bytcore.clasificador cla
        on cla.codigo_siaf = cm.id_clasificador
    left join bytsscom_bytcore.meta_institucional_siaf mis
        on cm.sec_func = mis.sec_func and cm.ano_eje = mis.id_anio::varchar
    inner join bytsscom_bytcore.meta_institucional mp
        on mis.id_meta_institucional = mp.id_meta_institucional
    left join certificado_anio_area areas
        on cf.certificado = areas.siaf_certificado and areas.ano_eje = cf.ano_eje
    group by cert.ano_eje,
            cf.glosa,
            cert.certificado,
            cf.secuencia,
            f.siaf_codigo,
            f.abre_fuente,
            cs.cod_doc,
            cs.num_doc,
            cla.codigo_siaf,
            cla.cod_clasif,
            substr(cla.cod_clasif, 0, 3),
            mis.id_meta_institucional,
            mis.sec_func,
            mp.nomb_met_ins,
            areas.cod_siaf_area,
            cs.estado_envio,
            cs.estado_registro,
            cs.fecha_creacion_clt;
    """
        },
          {
            "table": "sistema_informacion_gerencial.dm_expediente",
            "query": """
            select
                cs.anio as  ano_eje,
                ef.sec_ejec,
                ef.certificado,
                ef.expediente,
                ef.ciclo,
                ef.fase,
                ef.secuencia,
                ef.certificado_secuencia,
                ef.monto_nacional,
                ef.monto_saldo,
                ef.cod_doc_ref as cod_doc,
                ef.num_doc_ref as num_doc
                from sistema_informacion_gerencial.hechos_institucional_consolidados cs
                inner join bytsscom_bytsiaf.expediente_fase ef on cs.num_certificado = ef.certificado
                and ef.ano_eje = cs.anio::varchar
    """
        },
        {
            "table": "sistema_informacion_gerencial.hechos_pim",
            "query": """
                SELECT
                    id_periodo_pla,
                    f.id_fuente,
                    f.siaf_codigo as fuente_siaf,
                    p.id_area,
                    cod_siaf_area,
                    c.id_clasificador as id_generica,
                    c.nomb_clasif as generica,
                    monto_pia,
                    monto_pim
                    from bytsscom_bytpoa.vw_pim p
                inner join bytsscom_bytcore.area a
                    on p.id_area = a.id_area
                inner join bytsscom_bytcore.clasificador c
                on c.id_clasificador = p.id_clasificador
                inner join bytsscom_bytcore.fuente f
                on f.id_fuente = p.id_fuente;    
            """
        },
]

