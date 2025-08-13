from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "My API"
    API_V1_STR: str = "/api/v1"
    DATABASE_URL: str = "postgresql://speedy:root@localhost:5432/END"
    SECRET_KEY: str = "your-secret-key-here"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 550
    
    class Config:
        env_file = ".env"

settings = Settings()
