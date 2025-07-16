from pydantic_settings import BaseSettings
class Settings(BaseSettings):
    db_name: str
    db_user: str
    db_password: str
    db_host: str
    db_port: int
    data_dir: str
    quipushare : str 
    share_username: str
    share_password: str
    recurso : str
    file_name : str
    path_local : str
    path_extract : str
    class Config:
        env_file = ".env"

settings = Settings()


