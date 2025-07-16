import unicodedata
import re

def normalizar_texto(texto: str) -> str:
    # 1) Descomponer caracteres (NFKD) y eliminar diacríticos (tildes, etc.)
    txt = unicodedata.normalize('NFKD', texto)
    txt = ''.join(c for c in txt if not unicodedata.combining(c))

    # 2) Pasar a minúsculas
    txt = txt.lower()

    # 3) Eliminar cualquier carácter que no sea letra, dígito, espacio o guion bajo
    #    (aquí quitamos signos de puntuación, símbolos, etc.)
    txt = re.sub(r'[^a-z0-9\s_]', '', txt)

    # 4) Recortar espacios y reemplazar secuencias de espacios por un solo "_"
    txt = re.sub(r'\s+', '_', txt.strip())

    return txt.upper()
