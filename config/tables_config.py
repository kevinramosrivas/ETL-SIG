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
    "expediente":[  
    "ANO_EJE", "SEC_EJEC", "EXPEDIENTE", "MES_EJE", "COD_DOC", "NUM_DOC",
    "FECHA_DOC", "FECHA_ING", "USUARIO_ING", "FECHA_MOD", "USUARIO_MOD",
    "TIPO_OPERACION", "SEC_EJEC2", "MODALIDAD_COMPRA", "CLASE_MENOR_CUANTIA",
    "SEC_AREA", "FLAG_ENCARGO", "EXPEDIENTE_ENCARGANTE", "COD_MENSA",
    "ESTADO", "ESTADO_ENVIO", "ARCHIVO", "TIPO_PROCESO", "ID_PROCESO",
    "ID_CONTRATO", "SEC_EJEC_CONTRATO", "FASE_CONTRACTUAL", "PROCEDENCIA",
    "EXPEDIENTE_FINANCIAMIENTO"  
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
    "expediente_secuencia": [
        "ANO_EJE", "SEC_EJEC", "EXPEDIENTE", "CICLO", "FASE", "SECUENCIA",
        "CORRELATIVO","COD_DOC","NUM_DOC","FECHA_DOC","MONEDA","TIPO_CAMBIO",
        "MONTO","MONTO_SALDO","MONTO_NACIONAL","MONTO_EXTRANJERO","FECHA_ING",
        "USUARIO_ING","FECHA_MOD","USUARIO_MOD","NUM_RECORD","SERIE_DOC","ANO_PROCESO",
        "MES_PROCESO","DIA_PROCESO","GRUPO","EDICION","ANO_CTA_CTE","BANCO","CTA_CTE",
        "FECHA_AUTORIZACION","COD_MENSA","ESTADO_CTB","ESTADO_CTB_ANTERIOR","ESTADO",
        "ESTADO_ANTERIOR","ESTADO_ENVIO","ARCHIVO","REG_MULTIPLE","CTA_BCO_EJEC","FLG_INTERFASE",
        "IND_CONTABILIZA","TIPO_CAMBIO_PS","SEC_PROCESO","COD_DOC_B","FECHA_DOC_B", "NUM_DOC_B",
        "FECHA_BD_ORACLE","MES_AFECTACION_CALENDARIO","SECUENCIA_SOLICITUD","FECHA_CREACION_CLT",
        "FECHA_MODIFICACION_CLT","USUARIO_CREACION_CLT","USUARIO_MODIFICACION_CLT","FECHA_AUTORIZACION_GIRO",
        "VERIFICA_1"
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
    "expediente_secuencia": {
        "CORRELATIVO": "CORRELATIV",
        "TIPO_CAMBIO": "TIPO_CAMBI",
        "MONTO_SALDO": "MONTO_SALD",
        "MONTO_NACIONAL": "MONTO_NACI",
        "MONTO_EXTRANJERO": "MONTO_EXTR",
        "USUARIO_ING": "USUARIO_IN",
        "USUARIO_MOD": "USUARIO_MO",
        "ANO_CTA_CTE": "ANO_CTA_CT",
        "FECHA_AUTORIZACION": "FECHA_AUTO",
        "ESTADO_CTB_ANTERIOR": "ESTADO_CT2",
        "ESTADO_ANTERIOR": "ESTADO_ANT",
        "REG_MULTIPLE": "REG_MULTIP",
        "CTA_BCO_EJEC": "CTA_BCO_EJ",
        "FLG_INTERFASE": "FLG_INTERF",
        "IND_CONTABILIZA": "IND_CONTAB",
        "TIPO_CAMBIO_PS": "TIPO_CAMB2",
        "FECHA_DOC_B": "FECHA_DOC_",
        "NUM_DOC_B": "NUM_DOC_B",
        "FECHA_BD_ORACLE": "FECHA_BD_O",
        "MES_AFECTACION_CALENDARIO": "MES_AFECTA",
        "SECUENCIA_SOLICITUD": "SECUENCIA_",
        "FECHA_CREACION_CLT": "FECHA_CREA",
        "FECHA_MODIFICACION_CLT": "FECHA_MODI",
        "USUARIO_CREACION_CLT": "USUARIO_CR",
        "USUARIO_MODIFICACION_CLT": "USUARIO_M2",
        "FECHA_AUTORIZACION_GIRO": "FECHA_AUT2"
    },
    "expediente": {
        "USUARIO_ING": "USUARIO_IN",
        "USUARIO_MOD": "USUARIO_MO",
        "TIPO_OPERACION":"TIPO_OPERA",
        "MODALIDAD_COMPRA":"MODALIDAD_",
        "CLASE_MENOR_CUANTIA":"CLASE_MENO",
        "FLAG_ENCARGO": "FLAG_ENCAR",
        "EXPEDIENTE_ENCARGANTE":"EXPEDIENT2",
        "ESTADO_ENVIO": "ESTADO_ENV",
        "TIPO_PROCESO":"TIPO_PROCE",
        "ID_CONTRATO": "ID_CONTRAT",
        "SEC_EJEC_CONTRATO":"SEC_EJEC_C",
        "FASE_CONTRACTUAL":"FASE_CONTR",
        "PROCEDENCIA":"PROCEDENCI",
        "EXPEDIENTE_FINANCIAMIENTO":"EXPEDIENT3",
    }
   
    
}

# Lista mínima de metadatos para generar configs de Prefect
TABLES_INFO = [
    {
        "key": "certificado",
        "table": "bytsscom_bytsiaf.certificado",
        "path": "certificado.dbf",
        "filter_fields": {
            "CERTIFICAD": None,
            "ANO_EJE": ["2023","2024", "2025"],
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
            "ANO_EJE": ["2023","2024", "2025"],
        },
    },
    {
        "key": "certificado_secuencia",
        "table": "bytsscom_bytsiaf.certificado_secuencia",
        "path": "certificado_secuencia.dbf",
        "filter_fields": {
            "CERTIFICAD": None,
            "ANO_EJE": ["2023","2024", "2025"],
        },
    },
    {
        "key": "certificado_meta",
        "table": "bytsscom_bytsiaf.certificado_meta",
        "path": "certificado_meta.dbf",
        "filter_fields": {
            "CERTIFICAD": None,
            "ANO_EJE": ["2023","2024", "2025"],
        },
    },
    {
        "key": "expediente_fase",
        "table": "bytsscom_bytsiaf.expediente_fase",
        "path": "expediente_fase.dbf",
        "filter_fields": {
            "CERTIFICAD": None,
            "ANO_EJE": ["2023","2024", "2025"],
        },
    },
    {
        "key": "expediente_secuencia",
        "table": "bytsscom_bytsiaf.expediente_secuencia",
        "path": "expediente_secuencia.dbf",
        "filter_fields": {
            "ANO_EJE": ["2023","2024","2025"]
        },
    },
     {
        "key": "expediente",
        "table": "bytsscom_bytsiaf.expediente",
        "path": "expediente.dbf",
        "filter_fields": {
            "ANO_EJE": ["2023","2024","2025"],
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
               null as idsuperior
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
        upper(desc_fuente) as desc_fuente
    from bytsscom_bytcore.fuente
          where esta_fuente = '1' and id_fuente < 6
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
            ),
        certificado_consolidado as (
            select
                ano_eje,
                sec_ejec,
                certificado,
                sum(monto_nacional) as monto_detalle
                from
            bytsscom_bytsiaf.certificado_secuencia
            where cod_doc in ('086','000')
            group by ano_eje, sec_ejec, certificado
            )
        SELECT
            cc.ano_eje as anio,
            cc.certificado as num_certificado,
            coalesce(caa.cod_siaf_area, '0000') as cod_siaf_area,
            cc.monto_detalle as monto
            from
        certificado_consolidado cc
        left join certificado_anio_area caa
            on cc.certificado = caa.siaf_certificado
            and cc.ano_eje = caa.ano_eje
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
            ),
        certificado_consolidado as (
            select
                ano_eje,
                sec_ejec,
                certificado,
                secuencia,
                correlativo,
                cod_doc,
                num_doc,
                estado_envio,
                estado_registro,
                fecha_creacion_clt,
                sum(monto_nacional) as monto_detalle
                from
            bytsscom_bytsiaf.certificado_secuencia
            where cod_doc in ('086','000')
            group by ano_eje, sec_ejec, certificado,secuencia,correlativo
            ),
        fuentes as(
            select
                id_fuente,
                siaf_codigo,
                upper(desc_fuente) as desc_fuente
            from bytsscom_bytcore.fuente
                where esta_fuente = '1' and id_fuente < 6
        )
        SELECT
            cc.ano_eje,
            cc.certificado as num_certificado,
            coalesce(caa.cod_siaf_area, '0000') as cod_siaf_area,
            cc.secuencia,
            cc.sec_ejec,
            cm.monto_nacional as monto_clasificador,
            f.siaf_codigo as siaf_id_fuente,
            cf.glosa,
            cc.correlativo,
            cm.id_clasificador as siaf_id_clasificador,
            cla.cod_clasif AS clasificador,
            SUBSTRING(cla.cod_clasif, 1, 2) AS generica,
            cc.cod_doc,
            cc.num_doc,
            cc.estado_envio,
            cc.estado_registro,
            fecha_creacion_clt,
            mis.id_meta_institucional as idmeta,
            mis.sec_func as codmeta,
            mp.nomb_met_ins
            from
        certificado_consolidado cc
        left join certificado_anio_area caa
            on cc.certificado = caa.siaf_certificado
            and cc.ano_eje = caa.ano_eje
        inner join bytsscom_bytsiaf.certificado_fase cf
            on cc.certificado = cf.certificado
            and cc.ano_eje = cf.ano_eje
            and cc.secuencia = cf.secuencia
        inner join fuentes f
                on TRIM(cf.fuente_financ) = f.siaf_codigo
        inner join bytsscom_bytsiaf.certificado_meta cm
                on cc.certificado = cm.certificado
                and cc.ano_eje = cm.ano_eje
                and cc.secuencia = cm.secuencia
                and cc.correlativo = cm.correlativo
        inner join bytsscom_bytcore.clasificador cla
                on cla.codigo_siaf = cm.id_clasificador
                and cla.activo_clasif = 1
        left join bytsscom_bytcore.meta_institucional_siaf mis
                on cm.sec_func = mis.sec_func and cm.ano_eje = mis.id_anio::varchar
            inner join bytsscom_bytcore.meta_institucional mp
                on mis.id_meta_institucional = mp.id_meta_institucional
    """
        },
          {
            "table": "sistema_informacion_gerencial.dm_expediente",
            "query": """
    with expediente_generica as (
            SELECT distinct
                    c.cod_clasif AS cod_clasif,
                    c.codigo_siaf
            FROM bytsscom_bytcore.clasificador c
            ),
    expediente_consolidado as (
        SELECT
            es.ano_eje,
            es.expediente,
            es.sec_ejec,
            secuencia,
            correlativo,
            ciclo,
            coalesce(e.sec_area,'0000') as sec_area,
            fase,
            es.cod_doc,
            es.num_doc,
            es.estado_envio,
            es.fecha_autorizacion,
            sum(monto_nacional) AS monto_nacional
            FROM bytsscom_bytsiaf.expediente_secuencia es
            left JOIN bytsscom_bytsiaf.expediente e
                ON es.ano_eje = e.ano_eje
                AND es.expediente = e.expediente
            WHERE ciclo = 'G' and fase = 'D' and (estado_ctb = 'S' or estado_ctb is null )
            group by es.ano_eje,e.sec_area,es.sec_ejec,secuencia,correlativo,
            es.expediente,
            ciclo,
            fase
        )
        select
            ec.ano_eje as ano_eje,
            ec.sec_ejec,
            ec.sec_area as area_siaf,
            ec.expediente ,
            ec.fase,
            ec.secuencia,
            ec.correlativo,
            ec.ciclo,
            ef.certificado,
            ef.certificado_secuencia,
            ec.fecha_autorizacion,
            ef.fuente_financ as fuente_siaf,
            cla.cod_clasif AS clasificador,
            SUBSTRING(cla.cod_clasif, 1, 2) AS generica,
            em.monto_nacional,
            ec.cod_doc,
            ec.num_doc,
            em.estado_envio,
            em.id_clasificador as siaf_id_clasificador
        from  expediente_consolidado ec
        inner join bytsscom_bytsiaf.expediente_fase ef
            on ec.ano_eje = ef.ano_eje
            and ec.expediente = ef.expediente
            and ec.secuencia = ef.secuencia
        inner join bytsscom_bytsiaf.expediente_meta em
            on ec.ano_eje = em.ano_eje
            and ec.expediente = em.expediente
            and ec.secuencia = em.secuencia
            and ec.correlativo = em.correlativo
            and em.ciclo = 'G' and em.fase = 'D'
        left join expediente_generica cla
                on cla.codigo_siaf = em.id_clasificador
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

