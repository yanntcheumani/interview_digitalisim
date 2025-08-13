from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "My API"
    API_V1_STR: str = "/api/v1"
    DATABASE_URL: str = "postgresql://speedy:root@localhost:5432/END"
    SECRET_KEY: str = "your-secret-key-here"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 550
    CSV_COMMUNES_URL: str = "https://www.data.gouv.fr/fr/datasets/r/dbe8a621-a9c4-4bc3-9cae-be1699c5ff25"
    class Config:
        env_file = ".env"

settings = Settings()
