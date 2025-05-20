import aspose.zip as az
import os

ruta = "C:/Users/QUIPU-Q2005E-24/Desktop/ETL-SIG"

if os.path.exists(ruta):
    # Ubicar el archivo a descomprimir 
    with az.rar.RarArchive("DATA 24.04.25.rar") as archive:
    # Extraiga la carpeta del rar
     archive.extract_to_directory("C:/Users/QUIPU-Q2005E-24/Desktop/ETL-SIG")
else:
    print("No se encontró la ruta especificada.")


    


