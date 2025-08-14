"""
Pipeline ETL pour l'import des communes
Orchestration des étapes Extract, Transform, Load
"""

import logging
from sqlalchemy.orm import Session
from typing import Optional

from core.etl.extract import DataExtractor
from core.etl.transform import DataTransformer
from core.etl.load import DataLoader
from schemas.commune import ImportStats

# Configuration du logging
logger = logging.getLogger(__name__)


class CommunesETLPipeline:
    """Pipeline ETL complet pour l'import des communes"""
    
    def __init__(self, db_session: Session, csv_url: str = None):
        """
        Initialise le pipeline ETL
        
        Args:
            db_session: Session de base de données
        """
        self.db = db_session
        self.extractor = DataExtractor(csv_url=csv_url)
        self.transformer = DataTransformer()
        self.loader = DataLoader(db_session)
    
    def run_full_pipeline(self) -> ImportStats:
        """
        Runs the complete ETL pipeline

        Returns:
        Import statistics
        """
        logger.info("=== DÉBUT DU PIPELINE ETL ===")
        
        try:
            # EXTRACT : Téléchargement et lecture du CSV
            logger.info("Phase EXTRACT : Téléchargement du CSV")
            raw_df = self.extractor.extract_dataframe()
            
            if raw_df is None or raw_df.empty:
                error_msg = "Échec de l'extraction des données"
                logger.error(error_msg)
                return ImportStats(
                    total_processed=0,
                    total_imported=0,
                    total_updated=0,
                    errors=[error_msg]
                )
            
            logger.info(f"EXTRACT terminé : {len(raw_df)} lignes extraites")
            
            # TRANSFORM : Nettoyage et transformation
            logger.info("Phase TRANSFORM : Transformation des données")
            transformed_df = self.transformer.transform_data(raw_df)
            
            if transformed_df.empty:
                error_msg = "Aucune donnée valide après transformation"
                logger.error(error_msg)
                return ImportStats(
                    total_processed=len(raw_df),
                    total_imported=0,
                    total_updated=0,
                    errors=[error_msg]
                )
            
            logger.info(f"TRANSFORM terminé : {len(transformed_df)} lignes prêtes")
            
            logger.info("Phase LOAD : Chargement en base de données")
            communes_data = self.transformer.to_dict_list(transformed_df)

            stats = self.loader.load_communes(communes_data)
            
            logger.info(f"LOAD terminé : {stats.total_imported} créées, {stats.total_updated} mises à jour")
            
            logger.info("=== PIPELINE ETL TERMINÉ AVEC SUCCÈS ===")
            return stats
            
        except Exception as e:
            error_msg = f"Erreur critique dans le pipeline ETL : {str(e)}"
            logger.error(error_msg)
            return ImportStats(
                total_processed=0,
                total_imported=0,
                total_updated=0,
                errors=[error_msg]
            )
    