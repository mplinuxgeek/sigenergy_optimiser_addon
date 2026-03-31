# SigEnergy Optimiser

This repo now contains:
- `sigenergy_optimiser.yaml`: your existing Home Assistant blueprint.
- `optimizer/`: a standalone Python optimizer service with a FastAPI web UI.

## Web Interface (uvicorn)

The service now runs as a web app and background worker:
- Web UI with tabs: `Overview`, `Entities`, `Config`, `Logs`
- Background optimizer cycle runner
- Manual `Run Cycle Now` action from the UI

### Endpoints
- `/` tabbed web UI
- `/api/status` runtime status + key entity snapshot
- `/api/config` loaded config (token masked)
- `/api/logs` in-memory recent logs
- `POST /api/force-cycle` run a cycle immediately

## Run With Docker

1. Copy config template:

```sh
cp config.example.yaml config.yaml
```

2. Edit `config.yaml`:
- Set `home_assistant.url`
- Set `home_assistant.token` (long-lived access token)
- Adjust entities/thresholds to match your setup

3. Start container:

```sh
docker compose up -d --build
```

4. Open UI:
- `http://<host>:8000`

5. Watch logs:

```sh
docker compose logs -f sigenergy-optimizer
```

## Local Run (without Docker)

```sh
pip install -r requirements.txt
CONFIG_PATH=./config.yaml TZ=Australia/Adelaide python -m optimizer.main
```

## Notes

- Disable the original HA automation while testing this service to avoid two controllers writing to the same entities.
- The Python version is still a staged port of the large blueprint; keep conservative caps while validating behavior.
