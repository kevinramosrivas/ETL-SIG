import os
from prefect import flow, get_run_logger
from flows.extraccion import extraccion
from flows.transformacion_carga import trasnformacion_carga

@flow(name="ETL-SIG")
def etl_sig() -> None:
    logger = get_run_logger()
    logger.info(f"La ruta actual es: {os.getcwd()}")
    extraccion()
    trasnformacion_carga()
    logger.info("ETL finalizado.")

if __name__ == "__main__":
    etl_sig.serve(name="ETL-SIG-DEPLOYMENT", cron="0 0 * * *")
