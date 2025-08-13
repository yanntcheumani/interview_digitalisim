import pytest
import pandas as pd
from unittest.mock import patch

from core.etl.transform import DataTransformer


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({
        'code_postal': ['75001', '69001', '13001', '33000'],
        'nom_commune_complet': ['Paris', 'Lyon', 'Marseille', 'Bordeaux'],
        'population': [2161000, 515695, 861635, 254436],
        'autre_colonne': ['A', 'B', 'C', 'D']
    })


@pytest.fixture
def dirty_dataframe():
    return pd.DataFrame({
        'code_postal': ['75001', '69001', None, '1234', '13001', '75001'],
        'nom_commune_complet': ['Paris', '  lyon  ', 'Marseille', 'Test', 'MARSEILLE', 'Paris'],
        'population': [2161000, 515695, 861635, 123456, 861635, 2161000],
        'autre_colonne': ['A', 'B', 'C', 'D', 'E', 'F']
    })


@pytest.fixture
def transformer():
    return DataTransformer()


def test_init_transformer():
    transformer = DataTransformer()
    assert transformer is not None


def test_filter_required_columns_success(transformer, sample_dataframe):
    result = transformer.filter_required_columns(sample_dataframe)
    
    assert len(result.columns) == 2
    assert 'code_postal' in result.columns
    assert 'nom_commune_complet' in result.columns
    assert 'population' not in result.columns
    assert len(result) == 4


def test_filter_required_columns_missing_column(transformer):
    df_missing = pd.DataFrame({
        'code_postal': ['75001'],
        'population': [2161000]
        # nom_commune_complet manquant
    })
    
    with pytest.raises(ValueError) as exc_info:
        transformer.filter_required_columns(df_missing)
    
    assert "Colonnes manquantes" in str(exc_info.value)


def test_clean_data_success(transformer):
    df = pd.DataFrame({
        'code_postal': ['75001', '69001', '13001'],
        'nom_commune_complet': ['  paris  ', 'lyon', 'MARSEILLE']
    })
    
    result = transformer.clean_data(df)
    
    assert len(result) == 3
    assert result.iloc[0]['nom_commune_complet'] == 'PARIS'
    assert result.iloc[1]['nom_commune_complet'] == 'LYON'
    assert result.iloc[0]['code_postal'] == '75001'


def test_clean_data_removes_invalid_postal_codes(transformer):
    df = pd.DataFrame({
        'code_postal': ['75001', '1234', 'ABCDE', '69001'],
        'nom_commune_complet': ['Paris', 'Invalid1', 'Invalid2', 'Lyon']
    })
    
    result = transformer.clean_data(df)
    
    assert len(result) == 2
    assert result.iloc[0]['code_postal'] == '75001'
    assert result.iloc[1]['code_postal'] == '69001'


def test_clean_data_removes_null_values(transformer):
    df = pd.DataFrame({
        'code_postal': ['75001', None, '69001'],
        'nom_commune_complet': ['Paris', 'Test', None]
    })
    
    result = transformer.clean_data(df)
    
    assert len(result) == 1
    assert result.iloc[0]['code_postal'] == '75001'


def test_clean_data_removes_duplicates(transformer):
    df = pd.DataFrame({
        'code_postal': ['75001', '75001', '69001'],
        'nom_commune_complet': ['PARIS', 'PARIS', 'LYON']
    })
    
    result = transformer.clean_data(df)
    
    assert len(result) == 2


@patch('core.etl.transform.Commune.calculate_departement')
def test_add_departement_column(mock_calculate_dept, transformer):
    mock_calculate_dept.side_effect = ['75', '69', '13']
    
    df = pd.DataFrame({
        'code_postal': ['75001', '69001', '13001'],
        'nom_commune_complet': ['Paris', 'Lyon', 'Marseille']
    })
    
    result = transformer.add_departement_column(df)
    
    assert 'departement' in result.columns
    assert len(result) == 3
    assert result.iloc[0]['departement'] == '75'
    assert result.iloc[1]['departement'] == '69'
    assert result.iloc[2]['departement'] == '13'


def test_transform_data_complete_pipeline(transformer, sample_dataframe):
    with patch('core.etl.transform.Commune.calculate_departement') as mock_calculate:
        mock_calculate.side_effect = ['75', '69', '13', '33']
        
        result = transformer.transform_data(sample_dataframe)
        
        assert 'departement' in result.columns
        assert len(result.columns) == 3  # code_postal, nom_commune_complet, departement
        assert len(result) == 4


def test_transform_data_with_dirty_data(transformer, dirty_dataframe):
    with patch('core.etl.transform.Commune.calculate_departement') as mock_calculate:
        mock_calculate.side_effect = ['75', '69', '13']
        
        result = transformer.transform_data(dirty_dataframe)
        
        # Doit enlever les données invalides et doublons
        assert len(result) < len(dirty_dataframe)
        assert all(result['code_postal'].str.match(r'^\d{5}$'))
        assert all(result['nom_commune_complet'].str.isupper())


def test_to_dict_list_success(transformer):
    df = pd.DataFrame({
        'code_postal': ['75001', '69001'],
        'nom_commune_complet': ['PARIS', 'LYON'],
        'departement': ['75', '69']
    })
    
    result = transformer.to_dict_list(df)
    
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]['code_postal'] == '75001'
    assert result[0]['nom_commune_complet'] == 'PARIS'
    assert result[0]['departement'] == '75'


def test_to_dict_list_with_nan_values(transformer):
    df = pd.DataFrame({
        'code_postal': ['75001', '69001'],
        'nom_commune_complet': ['PARIS', 'LYON'],
        'departement': ['75', pd.NA]
    })
    
    result = transformer.to_dict_list(df)
    
    assert result[1]['departement'] == ''  # NaN converti en string vide


def test_to_dict_list_empty_dataframe(transformer):
    df = pd.DataFrame(columns=['code_postal', 'nom_commune_complet', 'departement'])
    
    result = transformer.to_dict_list(df)
    
    assert isinstance(result, list)
    assert len(result) == 0


def test_full_pipeline_integration(transformer):
    # Test d'intégration complet
    input_df = pd.DataFrame({
        'code_postal': ['75001', '69001', 'INVALID', '13001', '75001'],
        'nom_commune_complet': ['  paris  ', 'Lyon', 'BadCity', 'marseille', '  PARIS  '],
        'population': [2161000, 515695, 123, 861635, 2161000],
        'unwanted_column': ['A', 'B', 'C', 'D', 'E']
    })
    
    with patch('core.etl.transform.Commune.calculate_departement') as mock_calculate:
        mock_calculate.side_effect = ['75', '69', '13']
        
        # Pipeline complet
        filtered = transformer.filter_required_columns(input_df)
        cleaned = transformer.clean_data(filtered)
        with_dept = transformer.add_departement_column(cleaned)
        final_dict = transformer.to_dict_list(with_dept)
        
        assert len(final_dict) == 3  # Doublons et invalides supprimés
        assert all('departement' in record for record in final_dict)