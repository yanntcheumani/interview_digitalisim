from fastapi import FastAPI

from core.config import settings
from db.base import Base
from db.session import engine
from api.v1.router import api_v1

app = FastAPI(title=settings.PROJECT_NAME)

Base.metadata.create_all(bind=engine)

app.include_router(api_v1, prefix="/api/v1")

