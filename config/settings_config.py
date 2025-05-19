from pydantic_settings import BaseSettings
class Settings(BaseSettings):
    db_name: str
    db_user: str
    db_password: str
    db_host: str = "localhost"
    db_port: int = 5432
    data_dir: str = "DATA"

    class Config:
        env_file = ".env"

settings = Settings()