from pydantic_settings import BaseSettings
class Settings(BaseSettings):
    db_name: str
    db_user: str
    db_password: str
    db_host: str
    db_port: int
    sig_extraccion_anios_historicos: int
    mineco_amigable_base_url: str
    mineco_amigable_user_agent:str
    mineco_amigable_referer: str
    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()


