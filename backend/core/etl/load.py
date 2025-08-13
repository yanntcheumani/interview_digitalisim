import pandas as pd
from sqlalchemy.orm import Session
from typing import List, Dict, Any
import logging
from db.models.commune import Commune
from schemas.commune import ImportStats
from sqlalchemy import func

logger = logging.getLogger(__name__)


class DataLoader:
    
    def __init__(self, db_session: Session):

        self.db = db_session
    
    def load_communes(self, communes_data: List[Dict[str, Any]]) -> ImportStats:
        """
        Loads municipality data into database
        
        Args:
            municipality_data: List of dictionaries representing municipalities
            
        Returns:
            Import statistics
        """
        stats = ImportStats(
            total_processed=len(communes_data),
            total_imported=0,
            total_updated=0,
            errors=[]
        )

        logger.info(f"Début du chargement de {len(communes_data)} communes")
        
        for i, commune_data in enumerate(communes_data):
            try:
                existing_commune = self.db.query(Commune).filter(
                    Commune.postal_code == commune_data['code_postal'],
                    Commune.commune_name == commune_data['nom_commune_complet']
                ).first()
                
                if existing_commune:
                    existing_commune.departement = commune_data['departement']
                    stats.total_updated += 1
                    
                else:
                    new_commune = Commune(
                        postal_code=commune_data['code_postal'],
                        commune_name=commune_data['nom_commune_complet'],
                        departement=commune_data['departement']
                    )
                    self.db.add(new_commune)
                    stats.total_imported += 1
                
                if (i + 1) % 1000 == 0:
                    self.db.commit()
                    logger.info(f"Progression : {i + 1}/{len(communes_data)} communes traitées")
                    
            except Exception as e:
                error_msg = f"Erreur ligne {i+1}: {str(e)} - Données: {commune_data}"
                logger.error(error_msg)
                stats.errors.append(error_msg)

                # Rollback de la transaction en cours pour cette ligne
                self.db.rollback()
        
        try:
            self.db.commit()
            logger.info(f"Chargement terminé : {stats.total_imported} créées, {stats.total_updated} mises à jour")
        except Exception as e:
            logger.error(f"Erreur lors du commit final : {e}")
            self.db.rollback()
            stats.errors.append(f"Erreur commit final : {str(e)}")
        
        return stats

    def get_load_statistics(self) -> Dict[str, Any]:
        """
        Returns the current statistics from the database.
        
        Returns:
            Dictionary with statistics.
        """
        try:
            total_communes = self.db.query(Commune).count()
            
            dept_stats = (
                self.db.query(Commune.departement, func.count(Commune.id))
                .group_by(Commune.departement)
                .all()
            )
            
            return {
                "total_communes": total_communes,
                "departements_count": len(dept_stats),
                "top_departements": dict(sorted(dept_stats, key=lambda x: x[1], reverse=True)[:10])
            }
            
        except Exception as e:
            logger.error(f"Erreur lors du calcul des statistiques : {e}")
            return {"error": str(e)}