from fastapi.testclient import TestClient



def test_create_commune(sample_commune, client):
    response = client.post("/api/v1/commune/", json=sample_commune)
    assert response.status_code == 200
    assert response.json()["nom_commune_complet"] == "PARIS"

def test_create_another_commune(another_commune, client):
    response = client.post("/api/v1/commune/", json=another_commune)
    assert response.status_code == 200
    assert response.json()["nom_commune_complet"] == "LYON"

def test_update_commune(sample_commune, client):
    updated = sample_commune.copy()
    updated["code_postal"] = "75001"
    response = client.post("/api/v1/commune/", json=updated)
    assert response.status_code == 200
    assert response.json()["code_postal"] == "75001"

def test_get_commune_by_name(sample_commune, client):
    response = client.get(f"/communes/{sample_commune['nom_commune_complet']}")
    assert response.status_code == 200
    assert response.json()["code_postal"].startswith("75")

def test_get_commune_not_found(client):
    response = client.get("/communes/INEXISTANTE")
    assert response.status_code == 404

def test_get_communes_by_departement(client):
    response = client.get("/departements/75/communes")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert any(c["nom_commune_complet"] == "PARIS" for c in data)

def test_get_communes_by_invalid_departement(client):
    response = client.get("/departements/999/communes")
    assert response.status_code == 404

def test_create_invalid_commune(client):
    invalid_data = {
        "nom_commune_complet": "TESTVILLE",
        "code_postal": "ABCD",
        "departement": "75"
    }
    response = client.post("/communes", json=invalid_data)
    assert response.status_code == 422 

def test_create_commune_missing_field(client):
    incomplete_data = {
        "nom_commune_complet": "TESTVILLE"
    }
    response = client.post("/communes", json=incomplete_data)
    assert response.status_code == 422

def test_get_all_communes(client):
    response = client.get("/communes")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 2 
