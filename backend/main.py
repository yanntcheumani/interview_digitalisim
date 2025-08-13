from fastapi import FastAPI
from core.config import settings
from db.base import Base
from db.session import engine


app = FastAPI(title=settings.PROJECT_NAME)

Base.metadata.create_all(bind=engine)

