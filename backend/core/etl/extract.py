import pandas as pd
import requests
from typing import Optional
from core.config import settings
import logging

logger = logging.getLogger(__name__)


class DataExtractor:
    """Classe responsable de l'extraction des données externes"""
    
    def __init__(self, csv_url: str = None):
        """
        Initializes the extractor.

        Args:
            csv_url: URL of the CSV file (optional, uses the default config).
        """
        self.csv_url = csv_url or settings.csv_communes_url
    
    def download_csv(self, timeout: int = 30) -> Optional[str]:
        """
        Download the CSV file from the URL.
        
        Args:
            timeout: Timeout in seconds for the request.
            
        Returns:
            CSV content as a string or None in case of error.
        """
        try:
            logger.info(f"Téléchargement du CSV depuis : {self.csv_url}")
            
            response = requests.get(self.csv_url, timeout=timeout)
            response.raise_for_status()
            
            logger.info(f"CSV téléchargé avec succès ({len(response.content)} bytes)")
            return response.text
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur lors du téléchargement du CSV : {e}")
            return None
    
    def extract_dataframe(self) -> Optional[pd.DataFrame]:
        """
        Extract data from CSV into a pandas DataFrame
        
        Returns:
            pandas DataFrame or None in case of error
        """

        try:
            csv_content = self.download_csv()
            if csv_content is None:
                return None
            
            df = pd.read_csv(
                pd.io.common.StringIO(csv_content),
                encoding='utf-8',
                sep=',',
                dtype={'code_postal': str}
            )
            
            logger.info(f"DataFrame créé avec {len(df)} lignes et {len(df.columns)} colonnes")
            logger.info(f"Colonnes disponibles : {list(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction du DataFrame : {e}")
            return None
    
    def extract_from_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Extract data from a local CSV file (for testing purposes)
        
        Args:
            file_path: Path to the local CSV file
            
        Returns:
            pandas DataFrame or None in case of error
        """
        try:
            df = pd.read_csv(
                file_path,
                encoding='utf-8',
                sep=',',
                dtype={'code_postal': str}
            )
            
            logger.info(f"DataFrame extrait du fichier local : {len(df)} lignes")
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors de la lecture du fichier {file_path} : {e}")
            return None