import logging
from sqlalchemy import func
from typing import List, Optional

from schemas.commune import CommuneCreate, CommuneUpdate
from db.models.commune import Commune


logger = logging.getLogger(__name__)

def create_commune(db, commune_data: CommuneCreate) -> Commune:
        """
        Creates or updates a municipality.
        
        Args:
            municipality_data: Data for the municipality to be created.
            
        Returns:
            Municipality object created or updated.
        """

        existing_commune = get_commune_by_name_and_postal(
            db,
            commune_data.name, 
            commune_data.postalCode
        )
        
        if existing_commune:
            logger.info(f"Mise à jour de la commune existante : {commune_data.name}")
            return update_commune(db, existing_commune.id, commune_data)
        
        db_commune = Commune(
            postal_code=commune_data.postalCode,
            commune_name=commune_data.name.upper(),
            departement=commune_data.departement,
            latitude=commune_data.latitude,
            longitude=commune_data.longitude
        )
        
        db.add(db_commune)
        db.commit()
        db.refresh(db_commune)
        
        logger.info(f"Nouvelle commune créée : {db_commune.commune_name} (ID: {db_commune.id})")
        return db_commune

def get_commune_by_id(db, commune_id: int) -> Optional[Commune]:
    """
    Retrieves a municipality by its ID.
    
    Args:
        municipality_id: ID of the municipality.
        
    Returns:
        Municipality object or None if not found.
    """
    return db.query(Commune).filter(Commune.id == commune_id).first()

def get_commune_by_name(db, nom_commune: str) -> Optional[Commune]:
    """
    Retrieves a municipality by name (case-insensitive search)
    
    Args:
        municipality_name: Name of the municipality to search for
        
    Returns:
        Municipality object or None if not found
    """

    commune = db.query(Commune).filter(
        func.upper(Commune.commune_name) == nom_commune.upper()
    ).first()
    
    if commune:
        logger.info(f"Commune trouvée : {commune.commune_name}")
    else:
        logger.warning(f"Commune non trouvée : {nom_commune}")
    
    return commune

def get_commune_by_name_and_postal(db, nom_commune: str, postal_code: str) -> Optional[Commune]:
    """
    Retrieves a municipality by its name and postal code.
    
    Args:
        municipality_name: Name of the municipality.
        postal_code: Postal code.
        
    Returns:
        Municipality object or None if not found.
    """
    return db.query(Commune).filter(
        func.upper(Commune.commune_name) == nom_commune.upper(),
        Commune.postal_code == postal_code
    ).first()

def update_commune(db, commune_id: int, commune_update) -> Optional[Commune]:
    """
    Updates an existing municipality.
    
    Args:
        municipality_id: ID of the municipality to be updated.
        municipality_update: Update data.
        
    Returns:
        Updated municipality object or None if not found.
    """
    db_commune = get_commune_by_id(db, commune_id)
    if not db_commune:
        logger.warning(f"Commune non trouvée pour mise à jour : ID {commune_id}")
        return None
    
    # Update fields
    if hasattr(commune_update, 'name'):
        db_commune.commune_name = commune_update.name.upper()
    if hasattr(commune_update, 'postalCode'):
        db_commune.postal_code = commune_update.postalCode
        # Recalculate department when postal code changes
        db_commune.departement = Commune.calculate_departement(commune_update.postalCode)
    if hasattr(commune_update, 'departement'):
        db_commune.departement = commune_update.departement
    if hasattr(commune_update, 'latitude'):
        db_commune.latitude = commune_update.latitude
    if hasattr(commune_update, 'longitude'):
        db_commune.longitude = commune_update.longitude
    
    db.commit()
    db.refresh(db_commune)
    
    logger.info(f"Commune mise à jour : {db_commune.commune_name}")
    return db_commune
