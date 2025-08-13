import pytest
from unittest.mock import Mock, patch
from sqlalchemy.orm import Session

from core.etl.load import DataLoader
from db.models.commune import Commune
from schemas.commune import ImportStats


@pytest.fixture
def mock_db_session():
    return Mock(spec=Session)


@pytest.fixture
def sample_communes_data():
    return [
        {
            'code_postal': '75001',
            'nom_commune_complet': 'PARIS',
            'departement': '75'
        },
        {
            'code_postal': '69001',
            'nom_commune_complet': 'LYON',
            'departement': '69'
        },
        {
            'code_postal': '13001',
            'nom_commune_complet': 'MARSEILLE',
            'departement': '13'
        }
    ]


@pytest.fixture
def loader(mock_db_session):
    return DataLoader(mock_db_session)


def test_init_loader(mock_db_session):
    loader = DataLoader(mock_db_session)
    assert loader.db == mock_db_session


def test_load_communes_new_communes(loader, mock_db_session, sample_communes_data):
    # Mock : aucune commune existante
    mock_db_session.query().filter().first.return_value = None
    
    result = loader.load_communes(sample_communes_data)
    
    assert result.total_processed == 3
    assert result.total_imported == 3
    assert result.total_updated == 0
    assert len(result.errors) == 0
    assert mock_db_session.add.call_count == 3
    assert mock_db_session.commit.call_count == 1


def test_load_communes_existing_communes(loader, mock_db_session, sample_communes_data):
    # Mock : communes existantes
    existing_commune = Mock()
    mock_db_session.query().filter().first.return_value = existing_commune
    
    result = loader.load_communes(sample_communes_data)
    
    assert result.total_processed == 3
    assert result.total_imported == 0
    assert result.total_updated == 3
    assert len(result.errors) == 0
    assert mock_db_session.add.call_count == 0


def test_load_communes_mixed_new_and_existing(loader, mock_db_session):
    communes_data = [
        {'code_postal': '75001', 'nom_commune_complet': 'PARIS', 'departement': '75'},
        {'code_postal': '69001', 'nom_commune_complet': 'LYON', 'departement': '69'}
    ]
    
    # Premier appel : commune existante, deuxième : nouvelle commune
    existing_commune = Mock()
    mock_db_session.query().filter().first.side_effect = [existing_commune, None]
    
    result = loader.load_communes(communes_data)
    
    assert result.total_processed == 2
    assert result.total_imported == 1
    assert result.total_updated == 1
    assert len(result.errors) == 0


def test_load_communes_with_error(loader, mock_db_session):
    communes_data = [
        {'code_postal': '75001', 'nom_commune_complet': 'PARIS', 'departement': '75'}
    ]
    
    # Simuler une erreur lors de la création
    mock_db_session.query().filter().first.return_value = None
    mock_db_session.add.side_effect = Exception("Database error")
    
    result = loader.load_communes(communes_data)
    
    assert result.total_processed == 1
    assert result.total_imported == 0
    assert result.total_updated == 0
    assert len(result.errors) == 1
    assert "Database error" in result.errors[0]
    assert mock_db_session.rollback.called


def test_load_communes_commit_every_1000(loader, mock_db_session):
    # Créer 1500 communes pour tester le commit périodique
    communes_data = []
    for i in range(1500):
        communes_data.append({
            'code_postal': f'{i:05d}',
            'nom_commune_complet': f'COMMUNE_{i}',
            'departement': f'{i//1000 + 1:02d}'
        })
    
    mock_db_session.query().filter().first.return_value = None
    
    loader.load_communes(communes_data)
    
    # Doit committer à 1000 et à la fin
    assert mock_db_session.commit.call_count == 2


def test_load_communes_final_commit_error(loader, mock_db_session, sample_communes_data):
    mock_db_session.query().filter().first.return_value = None
    mock_db_session.commit.side_effect = Exception("Commit error")
    
    result = loader.load_communes(sample_communes_data)
    
    assert len(result.errors) == 1
    assert "Commit error" in result.errors[0]
    assert mock_db_session.rollback.called


def test_get_load_statistics_success(loader, mock_db_session):
    # Mock des requêtes
    mock_db_session.query().count.return_value = 100
    
    dept_stats = [('75', 20), ('69', 15), ('13', 10)]
    mock_db_session.query().group_by().all.return_value = dept_stats
    
    result = loader.get_load_statistics()
    
    assert result['total_communes'] == 100
    assert result['departements_count'] == 3
    assert '75' in result['top_departements']
    assert result['top_departements']['75'] == 20


def test_get_load_statistics_with_sorting(loader, mock_db_session):
    mock_db_session.query().count.return_value = 50
    
    # Données non triées
    dept_stats = [('13', 5), ('75', 25), ('69', 15), ('33', 3)]
    mock_db_session.query().group_by().all.return_value = dept_stats
    
    result = loader.get_load_statistics()
    
    # Vérifier que c'est trié par ordre décroissant
    top_depts = list(result['top_departements'].items())
    assert top_depts[0] == ('75', 25)  # Le plus grand en premier
    assert top_depts[1] == ('69', 15)
    assert top_depts[2] == ('13', 5)


def test_get_load_statistics_top_10_limit(loader, mock_db_session):
    mock_db_session.query().count.return_value = 200
    
    # 15 départements pour tester la limite de 10
    dept_stats = [(f'{i:02d}', 100 - i) for i in range(15)]
    mock_db_session.query().group_by().all.return_value = dept_stats
    
    result = loader.get_load_statistics()
    
    assert len(result['top_departements']) == 10  # Maximum 10


def test_get_load_statistics_error(loader, mock_db_session):
    mock_db_session.query().count.side_effect = Exception("Database error")
    
    result = loader.get_load_statistics()
    
    assert 'error' in result
    assert 'Database error' in result['error']


def test_load_communes_empty_list(loader, mock_db_session):
    result = loader.load_communes([])
    
    assert result.total_processed == 0
    assert result.total_imported == 0
    assert result.total_updated == 0
    assert len(result.errors) == 0
    assert mock_db_session.commit.call_count == 1


def test_load_communes_creates_correct_commune_object(loader, mock_db_session):
    communes_data = [
        {'code_postal': '75001', 'nom_commune_complet': 'PARIS', 'departement': '75'}
    ]
    
    mock_db_session.query().filter().first.return_value = None
    
    with patch('core.etl.load.Commune') as mock_commune:
        loader.load_communes(communes_data)
        
        mock_commune.assert_called_once_with(
            postal_code='75001',
            commune_name='PARIS',
            departement='75'
        )


def test_load_communes_updates_existing_commune_correctly(loader, mock_db_session):
    communes_data = [
        {'code_postal': '75001', 'nom_commune_complet': 'PARIS', 'departement': '75'}
    ]
    
    existing_commune = Mock()
    existing_commune.departement = '74'  # Ancienne valeur
    mock_db_session.query().filter().first.return_value = existing_commune
    
    loader.load_communes(communes_data)
    
    assert existing_commune.departement == '75'  # Nouvelle valeur


def test_load_communes_query_filter_correctly(loader, mock_db_session, sample_communes_data):
    # Set up the mock chain properly to avoid extra calls during assertion
    query_mock = mock_db_session.query.return_value
    filter_mock = query_mock.filter.return_value
    filter_mock.first.return_value = None
    
    loader.load_communes(sample_communes_data)
    
    # Vérifier que query() a été appelé avec Commune
    mock_db_session.query.assert_called_with(Commune)
    
    # Vérifier que filter a été appelé (3 fois pour 3 communes)
    # Use the stored query_mock to avoid extra calls
    assert query_mock.filter.call_count == 3


@patch('core.etl.load.logger')
def test_load_communes_logs_correctly(mock_logger, loader, mock_db_session, sample_communes_data):
    mock_db_session.query().filter().first.return_value = None
    
    loader.load_communes(sample_communes_data)
    
    # Vérifier les logs
    mock_logger.info.assert_any_call("Début du chargement de 3 communes")
    mock_logger.info.assert_any_call("Chargement terminé : 3 créées, 0 mises à jour")