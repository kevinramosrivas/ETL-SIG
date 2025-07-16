# Paso 1: petición inicial y parseo de frames
from typing import Dict
from urllib.parse import urljoin
from bs4 import BeautifulSoup



def get_soup(session,url: str, data: Dict = None):
    resp = session.post(url, data=data) if data else session.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    # manejar frameset si existe
    frame = soup.find("frame", id="frame0") or soup.find("frame", {"name":"frame0"})
    if frame and frame.get("src"):
        return get_soup(urljoin(url, frame["src"]))
    return soup

# Paso 2: extraer payload y action de formulario ASP.NET
def extract_payload_and_action(url:str,soup):
    form = soup.find("form", id="aspnetForm")
    action = urljoin(url, form.get("action",""))
    payload = {
        inp["name"]: inp.get("value","")
        for inp in form.find_all("input", type="hidden")
        if inp.get("name")
    }
    return payload, action