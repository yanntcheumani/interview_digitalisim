from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Optional, List




class CommuneBase(BaseModel):
    name: str =  Field(..., min_length=5, max_length=5, description="Code postal à 5 chiffres")
    postalCode: str
    departement: str = Field(..., min_length=2, max_length=3, description="Numéro du département")

class CommuneCreate(CommuneBase):
    latitude: Optional[float] = Field(None, ge=-90, le=90, description="Latitude GPS")
    longitude: Optional[float] = Field(None, ge=-180, le=180, description="Longitude GPS")
    
    @field_validator('postalCode')
    def validate_code_postal(cls, v):
        if not v.isdigit():
            raise ValueError('Le code postal doit contenir uniquement des chiffres')
        return v
    
    @field_validator('name')
    def validate_nom_commune(cls, v):
        return v.strip().upper()


class CommuneOut(CommuneBase):
    id: int
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    
    model_config = ConfigDict(from_attributes=True)


class ImportStats(BaseModel):
    """Schéma pour les statistiques d'import"""
    total_processed: int = Field(..., description="Nombre de lignes traitées")
    total_imported: int = Field(..., description="Nombre de communes importées")
    total_updated: int = Field(..., description="Nombre de communes mises à jour")
    errors: List[str] = Field(default_factory=list, description="Liste des erreurs rencontrées")