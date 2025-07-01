# import smbclien
# import rarfile
# ----------------------------
# Tarea para descomprimir RAR
# ----------------------------
# @task(name="DESCOMPRIMIR-RAR")
# def descomprime_rar():
#     logger = get_run_logger()
#     logger.info("Iniciando descompresion de archivo RAR")
#     rarfile.UNRAR_TOOL = "UnRAR"
#     try:
#         ruta = settings.path_local
#         if os.path.exists(ruta):
#             with rarfile.RarFile(os.path.join(ruta, "DATA.rar")) as rf:
#                 rf.extractall(path=settings.path_extract)
#                 logger.info("Archivo descomprimido correctamente.")
#         else:
#             logger.error("No se encontro la ruta especificada.")
#     except Exception as e:
#         logger.error(f"Error al descomprimir el archivo: {e}")
#     return True
