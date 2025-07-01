from pydantic_settings import BaseSettings
class TableQuery(BaseSettings):
    table: str
    query:str

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
            coalesce(extract(year from es.fecha_mod),es.ano_eje::double precision) as anio_modificacion,
            sum(monto_nacional) AS monto_nacional
            FROM bytsscom_bytsiaf.expediente_secuencia es
            left JOIN bytsscom_bytsiaf.expediente e
                ON es.ano_eje = e.ano_eje
                AND es.expediente = e.expediente
            WHERE ciclo = 'G' and fase = 'D'
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
            and ef.fuente_financ != '88'
        inner join bytsscom_bytsiaf.expediente_meta em
            on ec.ano_eje = em.ano_eje
            and ec.expediente = em.expediente
            and ec.secuencia = em.secuencia
            and ec.correlativo = em.correlativo
            and em.ciclo = 'G' and em.fase = 'D'
        left join expediente_generica cla
                on cla.codigo_siaf = em.id_clasificador
    where certificado is not null AND ec.ano_eje::double precision = ec.anio_modificacion ;
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

