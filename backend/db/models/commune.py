from sqlalchemy import Column, Integer, String, Float, Index
from sqlalchemy.sql import func
from db.base import Base


class Commune(Base):
    """
    Model representing a French municipality
    
    Attributes:
        id: Unique auto-incrementing identifier
        postal_code: Postal code of the municipality
        commune_name: Full name of the municipality (in uppercase)
        departement: Department number

    """
    __tablename__ = "communes"

    id = Column(Integer, primary_key=True, index=True)

    postal_code = Column(String(5), nullable=False, index=True)
    
    commune_name = Column(String(255), nullable=False, index=True)
    
    departement = Column(String(3), nullable=False, index=True)
    
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)

    def __repr__(self):
        """Représentation string du modèle pour le debug"""
        return f"<Commune(nom='{self.commune_name}', postal_code='{self.postal_code}', dept='{self.departement}')>"

    @staticmethod
    def calculate_departement(postal_code: str) -> str:
        """
        Calculates the department number from the postal code.
        
        Rules:
        - Departments 01-95: first 2 digits
        - Corsica 2A/2B: postal codes 20xxx
        - DOM-TOM: first 3 digits for 97x and 98x
        
        Args:
            postal_code: 5-digit postal code
            
        Returns:
            Department number (string)
        """

        if not postal_code or len(postal_code) != 5:
            raise ValueError(f"Code postal invalide: {postal_code}")
        
        # Gestion de la Corse
        if postal_code.startswith("20"):
            # 20000-20199 = 2A, 20200-20999 = 2B
            if int(postal_code) < 20200:
                return "2A"
            else:
                return "2B"
        
        # DOM-TOM (Guadeloupe, Martinique, Guyane, etc.)
        if postal_code.startswith(("97", "98")):
            return postal_code[:3]
        
        return postal_code[:2]

