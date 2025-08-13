# app/db/session.py

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from core.config import settings

# Exemple avec PostgreSQL
SQLALCHEMY_DATABASE_URL = settings.DATABASE_URL

# Créer l'engine SQLAlchemy
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_pre_ping=True,  # Vérifie la validité de la connexion avant utilisation
)

# Créer une factory de session
SessionLocal = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)
