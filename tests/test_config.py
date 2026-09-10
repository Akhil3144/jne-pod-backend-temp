import json
from pathlib import Path

import main
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool


@pytest.fixture
def client(monkeypatch):
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    main.Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine)
    monkeypatch.setattr(main, "SessionLocal", sessions)
    monkeypatch.setenv("JNE_CONFIG_PASSWORD", "test-admin-password")
    with sessions() as db:
        row = main._get_sdk_config_row(db)
        legacy = main._bundled_sdk_config()
        legacy.update(configVersion="1.3", sdkEnabled=False)
        legacy["package"].pop("awbRequired", None)
        legacy.pop("delivered", None)
        legacy.pop("failedDelivery", None)
        row.configJson = legacy
        row.configVersion = "1.3"
        row.revision = 3
        db.commit()
    with TestClient(main.app) as test_client:
        yield test_client
    engine.dispose()


def save(client, config, path="/config/api", password="test-admin-password"):
    return client.post(path, json={"adminPassword": password, "config": config})


def test_legacy_13_get_adds_defaults_without_database_reset(client):
    result = client.get("/sdk-config").json()
    assert result["configVersion"] == "1.3"
    assert result["sdkEnabled"] is False
    assert result["package"]["awbRequired"] is True
    assert result["delivered"]["blurHandling"] == "ALLOW_WITH_PARTIAL_PERSON"
    assert result["failedDelivery"]["blurHandling"] == "INHERIT"
    with main.SessionLocal() as db:
        row = main._get_sdk_config_row(db)
        assert row.revision == 3
        assert "awbRequired" not in row.configJson["package"]


def test_config_has_all_controls_and_no_switch(client):
    response = client.get("/config")
    assert response.status_code == 200
    html = response.text
    assert "sdkEnabled" not in html
    assert "Kill Switch" not in html
    for field in ("personRequired", "acceptPartial", "acceptUncertain", "personConfidence", "packageRequired",
                  "packageConfidence", "awbRequired", "locationRequired", "acceptHouse", "acceptGate", "acceptBuilding",
                  "retakeBlur", "retakeDark", "gpsRequired", "gpsRadius", "requireDestination", "deliveredAlternatives",
                  "failedAlternatives", "deliveredBlur", "failedBlur"):
        assert f'id="{field}"' in html


def test_kill_switch_page_only_controls_sdk(client):
    html = client.get("/kill-switch").text
    assert 'id="enabled"' in html
    assert 'id="password"' in html
    assert "awbRequired" not in html
    assert "packageRequired" not in html
    assert "blurHandling" not in html


def test_config_save_preserves_switch_and_roundtrips_new_fields(client):
    config = client.get("/sdk-config").json()
    config["sdkEnabled"] = True
    config["package"]["awbRequired"] = False
    config["delivered"] = {"acceptedAlternatives": ["PACKAGE", "LOCATION"], "blurHandling": "RETAKE"}
    config["failedDelivery"] = {"acceptedAlternatives": ["PACKAGE_AND_PERSON"], "blurHandling": "ALLOW"}
    result = save(client, config)
    assert result.status_code == 200
    assert "sdkEnabled" not in result.json()
    saved = client.get("/sdk-config").json()
    assert saved["sdkEnabled"] is False
    assert saved["configVersion"] == "1.4"
    assert saved["package"]["awbRequired"] is False
    assert saved["delivered"] == config["delivered"]
    assert saved["failedDelivery"] == config["failedDelivery"]


def test_switch_save_preserves_all_config(client):
    before = client.get("/sdk-config").json()
    assert save(client, {"sdkEnabled": True}, "/kill-switch/api").status_code == 200
    after = client.get("/sdk-config").json()
    assert after["sdkEnabled"] is True
    for section in ("package", "person", "photo", "gps", "locationEvidence", "delivered", "failedDelivery"):
        assert after[section] == before[section]


@pytest.mark.parametrize("section,value", [("delivered", {"blurHandling": "TYPO"}),
                                           ("package", {"awbRequired": "false"}),
                                           ("failedDelivery", {"acceptedAlternatives": ["TYPO"]})])
def test_invalid_new_config_rejected_without_revision_change(client, section, value):
    config = client.get("/sdk-config").json()
    config[section] = value
    assert save(client, config).status_code == 400
    assert client.get("/sdk-config").json()["configVersion"] == "1.3"


def test_admin_auth_and_switch_field_isolation(client):
    config = client.get("/sdk-config").json()
    assert save(client, config, password="wrong").status_code == 401
    assert save(client, {"sdkEnabled": True, "package": {}}, "/kill-switch/api").status_code == 400
    assert client.get("/sdk-config").json()["sdkEnabled"] is False
