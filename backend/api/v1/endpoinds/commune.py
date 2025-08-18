from fastapi import APIRouter, status, HTTPException, Depends
import logging

from schemas.commune import CommuneCreate, CommuneOut
from deps import get_db
from crud.commune import create_commune, get_commune_by_name, get_commune_by_id, get_commune_by_name_and_postal
from sqlalchemy.orm import Session
from db.models.commune import Commune

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/", status_code=status.HTTP_200_OK, response_model=CommuneOut)
def create_or_update_commune(
    commune_data: CommuneCreate,
    db: Session = Depends(get_db)
) -> CommuneOut:
    """
    Creates or updates a municipality.
    
    - **postal_code**: 5-digit postal code.
    - **full_municipality_name**: Full name of the municipality.
    - **department**: Department number (calculated automatically if not provided).
    """
    try:
        # Calcul automatique du département si non fourni
        if not hasattr(commune_data, 'departement') or not commune_data.departement:
            commune_data.departement = Commune.calculate_departement(commune_data.postalCode)
        
        commune = create_commune(db, commune_data)

        logger.info(f"Commune créée/mise à jour : {commune.commune_name}")
        return commune

    except ValueError as e:
        logger.error(f"Erreur de validation : {e}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Données invalides : {str(e)}"
        )
    except Exception as e:
        logger.error(f"Erreur lors de la création/mise à jour : {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur interne du serveur"
        )


@router.get("/communes/{nom_commune}", status_code=status.HTTP_200_OK response_model=CommuneOut)
def api_get_commune_by_name(
    nom_commune: str,
    db: Session = Depends(get_db)
) -> CommuneOut:
    """
    Retrieves information about a municipality by name.
    
    - **municipality_name**: Name of the municipality to search for (case-insensitive).
    """
    commune = get_commune_by_name(db, nom_commune)
    
    if not commune:
        logger.warning(f"Commune non trouvée : {nom_commune}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Commune '{nom_commune}' non trouvée"
        )
    
    return commune