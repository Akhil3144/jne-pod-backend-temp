# JNE POD Temporary Backend

Temporary, Railway-ready FastAPI ingestion service and monitoring dashboard for Android JNE POD client testing.

## Local setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8090
```

Open `http://localhost:8090/dashboard`. Without `DATABASE_URL`, data is stored in `sqlite:///./jne_pod.db`.

## Railway

Set `DATABASE_URL` to the Railway PostgreSQL connection string. Both `postgres://` and `postgresql://` are accepted. Railway starts the service using:

```bash
uvicorn main:app --host 0.0.0.0 --port $PORT
```

The `Procfile` contains this command. Tables are created automatically at startup. Optionally set `CORS_ORIGINS` to a comma-separated allowlist; it defaults to `*` for temporary client/dashboard testing.

## API contract

- `GET /health` — service status
- `POST /api/pod` — ingest a POD event; only `eventId`, `idempotencyKey`, and `waybill` are required
- `GET /api/pod` — all events, newest first
- `GET /api/pod/{waybill}` — matching events, newest first
- `GET /dashboard` — responsive dashboard, filters, summaries, details, and 10-second refresh

Both `eventId` and `idempotencyKey` are database-unique. Replays return HTTP 200 with `duplicate: true`. Unknown SDK fields are tolerated and retained in `extraFields`. Common camelCase/snake_case aliases are normalized. A missing username remains null unless `courierName` is provided.

Distances are calculated with Haversine when the required coordinates exist. Backend distances remain null when no expected/backend destination is supplied. Destination-based compliance is recalculated consistently against `thresholdMeters` when possible.

## Verification

```bash
python3 -m compileall main.py tests
pytest -q
```

Tests use synthetic data and a temporary local SQLite database.
