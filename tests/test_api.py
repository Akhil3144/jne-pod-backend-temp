import importlib
import os

os.environ["DATABASE_URL"] = "sqlite:///./test_jne_pod.db"
import main
importlib.reload(main)

from fastapi.testclient import TestClient


def setup_function():
    main.Base.metadata.drop_all(main.engine)
    main.Base.metadata.create_all(main.engine)


def test_health_and_dashboard():
    with TestClient(main.app) as client:
        assert client.get("/health").json() == {"status": "ok", "service": "jne-pod-backend"}
        response = client.get("/dashboard")
        assert response.status_code == 200
        assert "JNE POD Dashboard" in response.text


def test_minimal_event_duplicate_and_no_synthetic_username():
    with TestClient(main.app) as client:
        payload = {"eventId": "e1", "idempotencyKey": "k1", "waybill": "WB1"}
        assert client.post("/api/pod", json=payload).json()["duplicate"] is False
        assert client.post("/api/pod", json=payload).json()["duplicate"] is True
        rows = client.get("/api/pod").json()
        assert len(rows) == 1
        assert rows[0]["username"] is None
        assert rows[0]["backendLatitude"] is None
        assert rows[0]["actionToBackendDistanceMeters"] is None
        assert rows[0]["photoToBackendDistanceMeters"] is None
        assert client.post("/api/pod", json={"waybill": "incomplete"}).status_code == 422


def test_compliance_variants_and_username_mapping():
    with TestClient(main.app) as client:
        values = [(True, "COMPLIANT"), (False, "NON_COMPLIANT"), (None, None), ("n/a", "N/A")]
        for i, (given, expected) in enumerate(values):
            p = {"eventId": f"e{i}", "idempotencyKey": f"k{i}", "waybill": f"WB{i}", "username": "real-courier", "complianceFlag": given}
            assert client.post("/api/pod", json=p).status_code == 200
        rows = client.get("/api/pod").json()
        assert {r["complianceFlag"] for r in rows} == {"COMPLIANT", "NON_COMPLIANT", "N/A", None}
        assert all(r["username"] == "real-courier" for r in rows)


def test_alias_courier_fallback_order_and_distances():
    with TestClient(main.app) as client:
        first = {"event_id": "old", "idempotency_key": "ko", "awb": "WB", "courier_name": "Courier A", "actionLatitude": -6.2, "actionLongitude": 106.8, "photoLatitude": -6.201, "photoLongitude": 106.801}
        second = {"eventId": "new", "idempotencyKey": "kn", "waybill": "WB", "username": "Courier B"}
        assert client.post("/api/pod", json=first).status_code == 200
        assert client.post("/api/pod", json=second).status_code == 200
        rows = client.get("/api/pod").json()
        assert [r["eventId"] for r in rows] == ["new", "old"]
        old = rows[1]
        assert old["username"] == "Courier A"
        assert old["actionToPhotoDistanceMeters"] > 0
        assert old["backendLatitude"] is None
        assert old["validationMode"] == "TWO_POINT"
        assert len(client.get("/api/pod/WB").json()) == 2


def test_outside_radius_comment_cannot_be_compliant():
    with TestClient(main.app) as client:
        payload = {"eventId": "edge", "idempotencyKey": "edge-key", "waybill": "EDGE", "complianceFlag": True, "geoMatchComment": "Outside delivery radius"}
        assert client.post("/api/pod", json=payload).status_code == 200
        assert client.get("/api/pod/EDGE").json()[0]["complianceFlag"] == "NON_COMPLIANT"
