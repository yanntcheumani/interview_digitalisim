
def test_create_commune(sample_commune, client):
    response = client.post("/api/v1/commune/", json=sample_commune)
    print(f"Status: {response.status_code}")
    print(f"Response: {response.text}")
    if response.status_code == 200:
        print(f"JSON: {response.json()}")
    assert response.status_code == 200
    # assert response.json()["name"] == "PARIS"

def test_create_another_commune(another_commune, client):
    response = client.post("/api/v1/commune/", json=another_commune)
    assert response.status_code == 200
    assert response.json()["commune_name"] == "LYON"

def test_update_commune(sample_commune, client):
    updated = sample_commune.copy()
    updated["postal_code"] = "75001"
    response = client.post("/api/v1/commune/", json=updated)
    assert response.status_code == 200
    assert response.json()["postal_code"] == "75001"

def test_get_commune_by_name(sample_commune, client):
    client.post("/api/v1/commune/", json=sample_commune)
    response = client.get(f"/api/v1/commune/communes/{sample_commune['name']}")
    assert response.status_code == 200
    assert response.json()["postal_code"].startswith("75")

def test_get_commune_not_found(client):
    response = client.get("api/v1/communes/INEXISTANTE")
    assert response.status_code == 404

def test_get_communes_by_departement(client):
    response = client.get("api/v1/departements/75/communes")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert any(c["name"] == "PARIS" for c in data)

def test_get_communes_by_invalid_departement(client):
    response = client.get("api/v1/departements/999/communes")
    assert response.status_code == 404

def test_create_invalid_commune(client):
    invalid_data = {
        "nom_commune_complet": "TESTVILLE",
        "code_postal": "ABCD",
        "departement": "75"
    }
    response = client.post("api/v1/commune/", json=invalid_data)
    assert response.status_code == 422 

def test_create_commune_missing_field(client):
    incomplete_data = {
        "nom_commune_complet": "TESTVILLE"
    }
    response = client.post("/", json=incomplete_data)
    assert response.status_code == 422

def test_get_all_communes(client):
    response = client.get("api/v1/communes")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 2 
