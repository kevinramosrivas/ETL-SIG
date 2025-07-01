# import smbclien
# import rarfile
# --------------------------------
# Tarea para copiar archivos SMB
# --------------------------------

# @task()
# def copy_file_from_share():
#     logger = get_run_logger()
#     logger.info("Iniciando copia de archivo desde recurso compartido")
#     try:
#         servidor = fr"{settings.quipushare}"
#         recurso = settings.recurso
#         archivo_nombre = settings.file_name
#         ruta_remota = fr"\\{servidor}\{recurso}\{archivo_nombre}"
#         ruta_local = os.path.join(settings.path_local, archivo_nombre)

#         smbclient.register_session(
#             servidor,
#             username=settings.share_username,
#             password=settings.share_password,
#         )
#         with smbclient.open_file(ruta_remota, mode='rb') as archivo_remoto:
#             with open(ruta_local, mode='wb') as archivo_local:
#                 shutil.copyfileobj(archivo_remoto, archivo_local)

#         logger.info(f"Archivo copiado a: {ruta_local}")
#     except Exception as e:
#         logger.error(f"Error al copiar el archivo: {e}")
#     return True