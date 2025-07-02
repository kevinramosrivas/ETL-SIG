
def crear_particiones(nombre_base: str, anios: list[int]) -> list[str]:
    sql_statements = []
 

    for anio in anios:
        nombre_particion = f"{nombre_base.split('.')[-1]}_{anio}"
        esquema = nombre_base.split('.')[0]
        nombre_completo_particion = f"{esquema}.{nombre_particion}"
        sql = f"""CREATE IF NO EXIST TABLE {nombre_completo_particion}
  PARTITION OF {nombre_base}
  FOR VALUES IN ({anio});"""
        sql_statements.append(sql)

    return sql_statements
