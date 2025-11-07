# SISTEMA DE INFORMACION GERENCIAL -  FLUJO DE ETL

Este proyecto implementa un Pipeline de Transformación y Carga (ETL) utilizando Prefect 3 para la orquestación y gestión de flujos de datos. El objetivo es automatizar la extracción de datos históricos de la Universidad Nacional Mayor de San Marcos (UNMSM), transformar la información y cargarla en una base de datos PostgreSQL para su análisis.

## Stack usado
| Componente | Función |
| :--- | :--- |
| **Prefect 3** | Orquestación, monitoreo y programación de flujos de trabajo. |
| **Docker / Docker Compose** | Contenedorización del entorno (Servidor Prefect, Base de Datos y Worker). |
| **Python** | Lenguaje de implementación para Tareas y Flujos. |
| **PostgreSQL** | Base de datos de persistencia para metadatos de Prefect y datos finales. |


## Pasos para levantar el proyecto
1. Clonar el repositorio
2. Crear un archivo .env con las siguientes variables de entorno:
```bash
    # ------------------------------------------------------------------------------
    # 1. CONFIGURACIÓN DEL SERVIDOR Y API DE PREFECT
    # ------------------------------------------------------------------------------

    # Credencial de autenticación (usuario:contraseña) para el Prefect Server (Uso interno).
    PREFECT_SERVER_API_AUTH_STRING="admin:secure_password123"

    # Credencial de autenticación (usuario:contraseña) que deben usar los clientes (workers, CLI) 
    # para comunicarse con el Prefect Server.
    PREFECT_API_AUTH_STRING="admin:secure_password123"

    # Deshabilita el contenido promocional dentro de la Interfaz de Usuario (UI) de Prefect.
    PREFECT_SERVER_UI_SHOW_PROMOTIONAL_CONTENT=false

    # ------------------------------------------------------------------------------
    # 2. CONEXIÓN A LA BASE DE DATOS DE METADATOS DE PREFECT
    # ------------------------------------------------------------------------------

    # Cadena de conexión completa que el Prefect Server utiliza para acceder a su 
    # base de datos de metadatos (PostgreSQL).
    PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://postgres:<secret_pasword>@localhost:5483/prefect"

    # ------------------------------------------------------------------------------
    # 3. CONEXIÓN A LA BASE DE DATOS DE DESTINO (DONDE SE CARGAN LOS DATOS ETL)
    # ------------------------------------------------------------------------------

    # Nombre de la base de datos de destino donde se cargarán los datos procesados.
    DB_NAME=bytsscom_unmsm

    # Usuario para acceder a la base de datos de destino.
    DB_USER=postgres

    # Contraseña para el usuario de la base de datos de destino.
    DB_PASSWORD=postgres

    # Host o dirección de la base de datos de destino (ej: 'host.docker.internal' 
    # para referirse a la máquina anfitriona desde Docker).
    DB_HOST=host.docker.internal

    # Puerto de la base de datos de destino.
    DB_PORT=5433

    # ------------------------------------------------------------------------------
    # 4. CONFIGURACIÓN DEL FLUJO Y EXTRACCIÓN DE DATOS
    # ------------------------------------------------------------------------------

    # Define la cantidad de años hacia atrás que la lógica del ETL debe consultar.
    SIG_EXTRACCION_ANOS_HISTORICOS=1

    # URL base de la fuente de datos (MINEC u otra).
    MINECO_AMIGABLE_BASE_URL=https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx

    # Cabecera HTTP: Identificador del cliente que realiza la solicitud web.
    MINECO_AMIGABLE_USER_AGENT="Mozilla/5.0"

    # Cabecera HTTP: Indica la página de origen de la solicitud web.
    MINECO_AMIGABLE_REFERER=https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx
```

3. Construir y levantar los contenedores
```bash
 # Detiene y elimina contenedores/volúmenes anteriores (-v elimina los datos de la DB)
docker compose down -v 

# Construye la imagen personalizada y levanta todos los servicios
docker compose up --build -d
```

