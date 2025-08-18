from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Optional, List




class CommuneBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255, description="Nom de la commune")
    postalCode: str = Field(..., min_length=5, max_length=5, description="Code postal à 5 chiffres")
    departement: str = Field(..., min_length=2, max_length=3, description="Numéro du département")

class CommuneCreate(CommuneBase):
    latitude: Optional[float] = Field(None, ge=-90, le=90, description="Latitude GPS")
    longitude: Optional[float] = Field(None, ge=-180, le=180, description="Longitude GPS")
    
    @field_validator('postalCode')
    def validate_postal_code(cls, v):
        if not v.isdigit():
            raise ValueError('Le code postal doit contenir uniquement des chiffres')
        return v

    @field_validator('name')
    def validate_name(cls, v):
        return v.strip().upper()

class CommuneUpdate(CommuneBase):
    code_postal: Optional[str] = Field(None, min_length=5, max_length=5)
    nom_commune_complet: Optional[str] = Field(None, min_length=1, max_length=255)
    departement: Optional[str] = Field(None, min_length=2, max_length=3)
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)
    
    @field_validator('postalCode')
    def validate_postal_code(cls, v):
        if v is not None and not v.isdigit():
            raise ValueError('Le code postal doit contenir uniquement des chiffres')
        return v
    
    @field_validator('name')
    def validate_name(cls, v):
        if v is not None:
            return v.strip().upper()
        return v

class CommuneOut(CommuneBase):
    id: int
    name: str = Field(..., alias="commune_name", min_length=1, max_length=255, description="Nom de la commune")
    postalCode: str = Field(..., alias="postal_code", min_length=5, max_length=5, description="Code postal à 5 chiffres")
    departement: str = Field(..., min_length=2, max_length=3, description="Numéro du département")
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class ImportStats(BaseModel):
    """Schéma pour les statistiques d'import"""
    total_processed: int = Field(..., description="Nombre de lignes traitées")
    total_imported: int = Field(..., description="Nombre de communes importées")
    total_updated: int = Field(..., description="Nombre de communes mises à jour")
    errors: List[str] = Field(default_factory=list, description="Liste des erreurs rencontrées")