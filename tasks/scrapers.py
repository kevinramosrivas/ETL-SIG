# tasks/scrapers.py

import re
import time
from typing import List

import pandas as pd
import requests
from urllib.parse import urljoin
from prefect import task, get_run_logger
from config.utils.textos import normalizar_texto
from config.utils.scraping_aspnet import extract_payload_and_action, get_soup
from config.env_config import settings


# Mapeo de código de fuente a valor real
FUENTES_MAP = {'1': '00', '2': '09','3':'19', '4': '13', '5': '18'}


@task(retries=2, retry_delay_seconds=10)
def scrape_and_load_pim(year: str, target_table: str) -> pd.DataFrame:
    HEADERS = {
        "User-Agent": settings.mineco_amigable_user_agent,
        "Referer": settings.mineco_amigable_referer
    }
    URL_BASE = settings.mineco_amigable_base_url
    """
    Toma un año y una tabla destino, hace todo el scraping de PIM→PÍA,
    concatena resultados y los carga vía COPY en la tabla destino.
    """
    logger = get_run_logger()
    start = time.time()
    logger.info(f"Iniciando scraping PIM para anio {year} en la tabla {target_table}")
    session = requests.Session()
    session.headers.update(HEADERS)

    init_url = f"{URL_BASE}?y={year}&ap=ActProy"
    soup = get_soup(session,init_url)
    payload, action = extract_payload_and_action(init_url,soup)

    for btn, grp, nivel in [
        ("ctl00$CPH1$BtnTipoGobierno", None,"Nivel de gobierno"),
        ("ctl00$CPH1$BtnSector", "E","Nivel: Gobierno Nacional"),
        ("ctl00$CPH1$BtnPliego", "10","Sector: Educacion"),
        ("ctl00$CPH1$BtnFuenteAgregada", "510","Pliego: UNMSM"),
    ]:
        payload.update({
            "ctl00$CPH1$DrpYear": year,
            btn: "x",
        })
        if grp:
            payload["grp1"] = grp
        logger.info(f"Realizando scraping en {nivel}")
        soup = get_soup(session,urljoin(URL_BASE, action), data=payload)
        payload, action = extract_payload_and_action(init_url,soup)

    df_fuentes = parse_html_table(soup)
    logger.info(f"Se encontraron {len(df_fuentes)} fuentes para el anio {year}")

    # Para cada fuente, obtener las genéricas y acumular
    all_dfs: List[pd.DataFrame] = []
    for idx, row in df_fuentes.iterrows():
        cod = row["CODIGO"]
        payload2 = payload.copy()
        payload2.update({
            "ctl00$CPH1$DrpYear": year,
            "ctl00$CPH1$BtnGenerica": "Genérica",
            "grp1": cod
        })
        soup2 = get_soup(session,urljoin(URL_BASE, action), data=payload2)
        df_gen = parse_html_table(soup2)
        df_gen["FUENTE_SIAF"] = FUENTES_MAP.get(cod, cod)
        all_dfs.append(df_gen)
        logger.info(f"Fuente {FUENTES_MAP[cod]}: {len(df_gen)} genericas")

    # Paso 6: concatenar todo y volcar a CSV en memoria
    final_df = pd.concat(all_dfs, ignore_index=True)
    final_df['ANIO'] = year
    final_df['GENERICA_SIAF'] = final_df['CODIGO'].apply(getCodigoGenerica)
    final_df.rename(columns={'PIM': 'MONTO_PIM'}, inplace=True)
    final_df.rename(columns={'PIA': 'MONTO_PIA'}, inplace=True)
    final_df["MONTO_PIA"] = final_df["MONTO_PIA"].str.replace(',', '', regex=False)
    final_df["MONTO_PIA"] = final_df["MONTO_PIA"].fillna(0)
    final_df["MONTO_PIM"] = final_df["MONTO_PIM"].str.replace(',', '', regex=False)
    final_df["MONTO_PIM"] = final_df["MONTO_PIM"].fillna(0)
    final_df["MONTO_PIA"] = final_df["MONTO_PIA"].astype(float)
    final_df["MONTO_PIM"] = final_df["MONTO_PIM"].astype(float)

    # Solo convierte a float si hay datos
    filtered_df = final_df[["ANIO","FUENTE_SIAF","GENERICA_SIAF","MONTO_PIA","MONTO_PIM"]]
    filtered_df = (
        filtered_df.groupby(
            ["ANIO", "FUENTE_SIAF", "GENERICA_SIAF"],
            as_index=False
        )
        .agg({
            "MONTO_PIA": "sum",
            "MONTO_PIM": "sum"
        })
    )

    elapsed = time.time() - start
    logger.info(f"Scraping completado en {elapsed:.2f}s")

    return filtered_df



def parse_html_table(soup):
    tabla_encabezado_soup = soup.select_one('#ctl00_CPH1_Mt0')
    if not tabla_encabezado_soup:
        return None

    filas_encabezado = tabla_encabezado_soup.find_all('tr')
    encabezados = []
    if len(filas_encabezado) >= 2:
        # La estructura de encabezados es compleja y a veces abarca dos filas.
        # Por ejemplo, "Ejecución" se divide en "Monto", "%" y "Saldo".
        fila1_celdas = filas_encabezado[0].find_all('td')
        fila2_celdas = filas_encabezado[1].find_all('td')

        # Iteramos por la primera fila para construir la lista final de encabezados
        idx_fila2 = 0
        for celda in fila1_celdas:
            texto = celda.get_text(strip=True)
            colspan = int(celda.get('colspan', 1))

            if colspan > 1:
                # Si una celda tiene colspan, sus sub-encabezados están en la segunda fila.
                for _ in range(colspan):
                    if idx_fila2 < len(fila2_celdas):
                        encabezados.append(normalizar_texto(str(fila2_celdas[idx_fila2].get_text(strip=True))))
                        idx_fila2 += 1
            else:
                encabezados.append(normalizar_texto(str(texto)))

    # Renombramos la primera columna, que siempre es el código.
    if encabezados:
        encabezados[0] = 'CODIGO'

    # --- 2. Extraer Filas de Datos ---
    tabla_datos_soup = soup.find('table', class_='Data')
    if not tabla_datos_soup:
        return None

    filas_datos = []
    for tr in tabla_datos_soup.find_all('tr'):
        onclick_attr = tr.get("onclick", "")
        # Usamos regex para extraer el 'kCod' del atributo javascript 'onclick'.
        # Ej: onclick="kCod=\"1\";tr_clk(0, this)" -> extrae "1"
        match = re.search(r'kCod\s*=\s*"([^"]+)"', onclick_attr)

        if not match:
            continue # Si no hay kCod, no es una fila de datos que nos interese.

        kcod = match.group(1)
        celdas = [td.get_text(strip=True) for td in tr.find_all('td')]

        if celdas:
            celdas[0] = kcod  # Reemplazamos el contenido visual con el código real.
            filas_datos.append(celdas)

    if not filas_datos:
        return None

    return pd.DataFrame(filas_datos, columns=encabezados)


def getCodigoGenerica(codigo:str) -> str:
    array_codigo = codigo.split("-")
    return "".join(array_codigo[1:])