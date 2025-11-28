from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "Visor Geoespacial ISED"
    VERSION: str = "0.1.0"
    API_PREFIX: str = "/api/v1"

    class Config:
        case_sensitive = True

settings = Settings()