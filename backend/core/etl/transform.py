import pandas as pd
from typing import List, Dict, Any
import logging
from db.models.commune import Commune

logger = logging.getLogger(__name__)


class DataTransformer:
    """Classe responsable de la transformation des données"""
    
    def __init__(self):
        """Initialise le transformateur"""
        pass
    
    def filter_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters the DataFrame to keep only the necessary columns.
        
        Args:
            df: Source DataFrame.
            
        Returns:
            DataFrame with only the required columns.
        """
        required_columns = ['code_postal', 'nom_commune_complet']
        
        # Vérification de la présence des colonnes
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Colonnes manquantes : {missing_columns}")
            logger.info(f"Colonnes disponibles : {list(df.columns)}")
            raise ValueError(f"Colonnes manquantes dans le CSV : {missing_columns}")
        
        # Filtrage des colonnes
        filtered_df = df[required_columns].copy()
        logger.info(f"Filtrage effectué : {len(filtered_df)} lignes conservées")
        
        return filtered_df
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the data in the DataFrame.
        
        Args:
            df: DataFrame to be cleaned.
            
        Returns:
            Cleaned DataFrame.
        """
        cleaned_df = df.copy()
        
        # Suppression des lignes avec des valeurs manquantes critiques
        initial_count = len(cleaned_df)
        cleaned_df = cleaned_df.dropna(subset=['code_postal', 'nom_commune_complet'])
        
        cleaned_df['code_postal'] = cleaned_df['code_postal'].astype(str).str.strip()
        
        # Filtrage des codes postaux valides (5 chiffres)
        cleaned_df = cleaned_df[cleaned_df['code_postal'].str.match(r'^\d{5}$')]
        
        # Nettoyage et mise en majuscules des noms de communes
        cleaned_df['nom_commune_complet'] = (
            cleaned_df['nom_commune_complet']
            .astype(str)
            .str.strip()
            .str.upper()
        )
        
        cleaned_df = cleaned_df.drop_duplicates(subset=['code_postal', 'nom_commune_complet'])
        
        final_count = len(cleaned_df)
        logger.info(f"Nettoyage terminé : {initial_count} -> {final_count} lignes "
                   f"({initial_count - final_count} lignes supprimées)")
        
        return cleaned_df
    
    def add_departement_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds the department column calculated from the postal code.
        
        Args:
            df: DataFrame with the postal_code column.
            
        Returns:
            DataFrame with the department column added.
        """
        df_with_dept = df.copy()
        
        # Use lambda wrapper to handle mocking properly
        df_with_dept['departement'] = df_with_dept['code_postal'].apply(
            lambda x: Commune.calculate_departement(x)
        )

        logger.debug(f"Département ajoutée!!!")

        return df_with_dept
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Complete data transformation pipeline

        Args:
        df: Source DataFrame

        Returns:
        Transformed DataFrame ready for import
        """
        logger.info("Début de la transformation des données")
        
        filtered_df = self.filter_required_columns(df)
        
        cleaned_df = self.clean_data(filtered_df)
        
        final_df = self.add_departement_column(cleaned_df)
        
        logger.info(f"Transformation terminée : {len(final_df)} communes prêtes à importer")
        
        return final_df
    
    def to_dict_list(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Converts the DataFrame into a list of dictionaries.
        
        Args:
            df: DataFrame to convert.
            
        Returns:
            List of dictionaries representing municipalities.
        """

        # Conversion en dictionnaires avec gestion des valeurs NaN
        records = df.fillna('').to_dict('records')
        
        logger.info(f"Conversion en dictionnaires : {len(records)} enregistrements")
        
        return records