from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import logging
from fastapi.middleware.cors import CORSMiddleware

from core.config import settings
from db.base import Base
from db.session import SessionLocal, engine
from api.v1.router import api_v1
from core.etl import CommunesETLPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


app = FastAPI(title=settings.PROJECT_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

Base.metadata.create_all(bind=engine)

app.include_router(api_v1, prefix="/api/v1")

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc: HTTPException):
    """Gestionnaire pour les HTTPException"""
    logger.info(f"HTTPException: {exc.status_code} - {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc: Exception):
    """Gestionnaire pour les erreurs générales"""
    logger.error(f"Erreur inattendue : {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Erreur interne du serveur"}
    )


def setup_villa():
    db = SessionLocal()

    etl = CommunesETLPipeline(db, settings.CSV_COMMUNES_URL)
    etl.run_full_pipeline()

setup_villa()

    