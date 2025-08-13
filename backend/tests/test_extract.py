import pytest
import pandas as pd
import requests
from unittest.mock import Mock, patch

from core.etl.extract import DataExtractor


@pytest.fixture
def sample_csv_data():
    return """nom,code_postal,population
Paris,75001,2161000
Lyon,69001,515695"""


@pytest.fixture  
def extractor():
    return DataExtractor("https://example.com/test.csv")


def test_init_with_url():
    extractor = DataExtractor("https://test.com/data.csv")
    assert extractor.csv_url == "https://test.com/data.csv"


def test_init_without_url():
    with patch('core.etl.extract.settings') as mock_settings:
        mock_settings.csv_communes_url = "https://default.com/data.csv"
        extractor = DataExtractor()
        assert extractor.csv_url == "https://default.com/data.csv"


@patch('core.etl.extract.requests.get')
def test_download_csv_success(mock_get, sample_csv_data, extractor):
    mock_response = Mock()
    mock_response.text = sample_csv_data
    mock_response.content = sample_csv_data.encode()
    mock_get.return_value = mock_response
    
    result = extractor.download_csv()
    assert result == sample_csv_data


@patch('core.etl.extract.requests.get')
def test_download_csv_fails(mock_get, extractor):
    mock_get.side_effect = requests.exceptions.RequestException("Network error")
    
    result = extractor.download_csv()
    assert result is None


@patch('core.etl.extract.requests.get')
def test_download_csv_with_timeout(mock_get, sample_csv_data, extractor):
    mock_response = Mock()
    mock_response.text = sample_csv_data
    mock_response.content = sample_csv_data.encode()
    mock_get.return_value = mock_response
    
    extractor.download_csv(timeout=60)
    mock_get.assert_called_with("https://example.com/test.csv", timeout=60)


@patch.object(DataExtractor, 'download_csv')
@patch('core.etl.extract.pd.read_csv')
def test_extract_dataframe_success(mock_read_csv, mock_download, extractor, sample_csv_data):
    mock_download.return_value = sample_csv_data
    mock_df = pd.DataFrame({'nom': ['Paris'], 'code_postal': ['75001']})
    mock_read_csv.return_value = mock_df
    
    result = extractor.extract_dataframe()
    assert result is not None
    assert len(result) == 1


@patch.object(DataExtractor, 'download_csv')
def test_extract_dataframe_download_fails(mock_download, extractor):
    mock_download.return_value = None
    
    result = extractor.extract_dataframe()
    assert result is None


@patch.object(DataExtractor, 'download_csv')
@patch('core.etl.extract.pd.read_csv')
def test_extract_dataframe_pandas_fails(mock_read_csv, mock_download, extractor, sample_csv_data):
    mock_download.return_value = sample_csv_data
    mock_read_csv.side_effect = Exception("Pandas error")
    
    result = extractor.extract_dataframe()
    assert result is None


@patch('core.etl.extract.pd.read_csv')
def test_extract_from_file_success(mock_read_csv, extractor):
    mock_df = pd.DataFrame({'nom': ['Paris'], 'code_postal': ['75001']})
    mock_read_csv.return_value = mock_df
    
    result = extractor.extract_from_file("test.csv")
    assert result is not None
    assert len(result) == 1


@patch('core.etl.extract.pd.read_csv')
def test_extract_from_file_fails(mock_read_csv, extractor):
    mock_read_csv.side_effect = FileNotFoundError("File not found")
    
    result = extractor.extract_from_file("missing.csv")
    assert result is None


@patch('core.etl.extract.pd.read_csv')
def test_extract_from_file_with_correct_params(mock_read_csv, extractor):
    mock_df = pd.DataFrame()
    mock_read_csv.return_value = mock_df
    
    extractor.extract_from_file("test.csv")
    
    mock_read_csv.assert_called_with(
        "test.csv",
        encoding='utf-8',
        sep=',',
        dtype={'code_postal': str}
    )


def test_extract_dataframe_real_csv_format():
    # Test avec de vraies données CSV
    csv_content = "nom,code_postal,population\nParis,75001,2161000"
    
    with patch.object(DataExtractor, 'download_csv') as mock_download:
        mock_download.return_value = csv_content
        
        extractor = DataExtractor("https://test.com")
        result = extractor.extract_dataframe()
        
        assert result is not None
        assert result.iloc[0]['nom'] == 'Paris'
        assert result.iloc[0]['code_postal'] == '75001'
        assert result.iloc[0]['population'] == 2161000