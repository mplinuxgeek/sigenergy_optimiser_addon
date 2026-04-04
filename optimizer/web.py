from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import secrets
import threading
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request, Security, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from fastapi.templating import Jinja2Templates

from optimizer.config import AppConfig
from optimizer.runtime import MemoryLogHandler, OptimizerRuntime


def setup_logging() -> MemoryLogHandler:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    handler = MemoryLogHandler(limit=500)
    handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(handler)
    return handler


CONFIG_PATH = os.environ.get("CONFIG_PATH", "/app/config.yaml")
TIMEZONE = os.environ.get("TZ", "Australia/Adelaide")
LOG_HANDLER = setup_logging()
RUNTIME = OptimizerRuntime(CONFIG_PATH, TIMEZONE)

# ---------------------------------------------------------------------------
# Optional API key authentication
# Set the API_KEY environment variable to require authentication on all
# state-changing POST endpoints.  If unset, the API is open (suitable for
# trusted local network deployments).
# ---------------------------------------------------------------------------
_API_KEY_ENV = os.environ.get("API_KEY", "").strip()
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def _require_api_key(key: str | None = Security(_api_key_header)) -> None:
    if not _API_KEY_ENV:
        return  # Auth disabled; open access
    if not key or not secrets.compare_digest(key, _API_KEY_ENV):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


_AUTH = [Depends(_require_api_key)]


def _validate_float(name: str, value: float, *, min_val: float | None = None, max_val: float | None = None) -> float:
    """Reject NaN, Inf, and out-of-range floats from API inputs."""
    if math.isnan(value) or math.isinf(value):
        raise ValueError(f"{name} must be a finite number, got {value}")
    if min_val is not None and value < min_val:
        raise ValueError(f"{name} must be >= {min_val}, got {value}")
    if max_val is not None and value > max_val:
        raise ValueError(f"{name} must be <= {max_val}, got {value}")
    return value

app = FastAPI(title="SigEnergy Optimizer")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

# ---------------------------------------------------------------------------
# WebSocket push broadcaster
# The optimizer worker is a background thread; asyncio lives in the main thread.
# We hold a reference to the event loop so we can schedule sends from the
# worker thread using call_soon_threadsafe.
# ---------------------------------------------------------------------------
_ws_queues: set[asyncio.Queue] = set()
_ws_lock = threading.Lock()
_event_loop: asyncio.AbstractEventLoop | None = None
# Populated in on_startup; maps HA entity_id → logical key used in WS messages.
_key_entity_map: dict[str, str] = {}


def _build_key_entity_map() -> dict[str, str]:
    _e = RUNTIME.cfg.entities
    return {
        _e.battery_soc_sensor: "battery_soc",
        _e.pv_power_sensor: "pv_power",
        _e.consumed_power_sensor: "load_power",
        _e.grid_export_power_sensor: "grid_export_power",
        _e.grid_import_power_sensor: "grid_import_power",
        _e.battery_power_sensor: "battery_power",
        _e.price_sensor: "price",
        _e.feedin_sensor: "feedin",
        _e.ems_mode_select: "mode",
        _e.grid_export_limit: "grid_export_limit",
        _e.grid_import_limit: "grid_import_limit",
        _e.pv_max_power_limit: "pv_max_power_limit",
        _e.forecast_today_sensor: "forecast_today",
        _e.forecast_tomorrow_sensor: "forecast_tomorrow",
        _e.forecast_remaining_sensor: "forecast_remaining",
        _e.ha_control_switch: "ha_control",
    }


def _refresh_key_entity_map() -> None:
    global _key_entity_map
    _key_entity_map = {k: v for k, v in _build_key_entity_map().items() if k}


def _config_file_info() -> dict[str, str | int | None]:
    path = Path(CONFIG_PATH).resolve()
    stat = path.stat()
    return {
        "path": str(path),
        "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(timespec="seconds"),
        "size_bytes": stat.st_size,
    }


def _load_raw_config() -> dict[str, object]:
    path = Path(CONFIG_PATH).resolve()
    return {
        **_config_file_info(),
        "content": path.read_text(encoding="utf-8"),
    }


def _validate_config_content(content: str) -> dict[str, object]:
    path = Path(CONFIG_PATH).resolve()
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".yaml",
            prefix=f"{path.stem}.validate.",
            dir=str(path.parent),
            delete=False,
        ) as tmp:
            tmp.write(content)
            tmp_path = Path(tmp.name)
        cfg = AppConfig.load(str(tmp_path))
        cfg.validate()
        return {
            "ok": True,
            "config": {
                "home_assistant": {"url": cfg.ha_url, "token": "***"},
                "service": cfg.service.__dict__,
                "entities": cfg.entities.__dict__,
                "thresholds": cfg.thresholds.__dict__,
                "profile_overrides": cfg.profile_overrides,
            },
        }
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass


def _write_config_atomic(content: str) -> dict[str, str | int | None]:
    path = Path(CONFIG_PATH).resolve()
    tmp_path = path.with_name(f".{path.name}.{secrets.token_hex(4)}.tmp")
    tmp_path.write_text(content, encoding="utf-8")
    tmp_path.replace(path)
    return _config_file_info()


def _ws_broadcast(msg: str) -> None:
    """Called from the optimizer worker thread after each cycle."""
    if _event_loop is None:
        return
    with _ws_lock:
        queues = list(_ws_queues)
    for q in queues:
        _event_loop.call_soon_threadsafe(q.put_nowait, msg)


@app.on_event("startup")
async def on_startup() -> None:
    global _event_loop, _key_entity_map
    _event_loop = asyncio.get_running_loop()
    RUNTIME.add_cycle_listener(lambda: _ws_broadcast(json.dumps({"type": "cycle"})))

    # Build a map of entity_id -> logical key for all entities shown in the UI.
    # State changes to these entities are pushed to browser clients immediately.
    _refresh_key_entity_map()

    def _on_ha_state(entity_id: str, state: str, attributes: dict) -> None:
        key = _key_entity_map.get(entity_id)
        if key is None:
            return
        _ws_broadcast(json.dumps({
            "type": "ha_state",
            "entity_id": entity_id,
            "key": key,
            "state": state,
            # Include unit_of_measurement so the browser can format values correctly
            # (e.g. W vs kW for power sensors).
            "unit": attributes.get("unit_of_measurement", ""),
        }))

    RUNTIME.client.add_state_listener(_on_ha_state)
    RUNTIME.start()


@app.on_event("shutdown")
def on_shutdown() -> None:
    RUNTIME.stop()


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    q: asyncio.Queue = asyncio.Queue()
    with _ws_lock:
        _ws_queues.add(q)

    # Immediately push the current state of every monitored entity so the
    # browser cards are populated the instant the WebSocket connects —
    # no waiting for the next HA state_changed event or optimizer cycle.
    try:
        all_states = RUNTIME.client.get_all_states()
        for entity_id, key in _key_entity_map.items():
            state_obj = all_states.get(entity_id)
            if state_obj:
                await ws.send_text(json.dumps({
                    "type": "ha_state",
                    "entity_id": entity_id,
                    "key": key,
                    "state": state_obj.state,
                    "unit": state_obj.attributes.get("unit_of_measurement", ""),
                }))
    except Exception:
        pass

    try:
        while True:
            msg = await q.get()
            try:
                await ws.send_text(msg)
            except Exception:
                break  # Client disconnected mid-send; exit cleanly
    except WebSocketDisconnect:
        pass
    finally:
        with _ws_lock:
            _ws_queues.discard(q)


@app.get("/health")
def health() -> JSONResponse:
    status = RUNTIME.status()
    last_completed = status.get("last_cycle_completed")
    last_error = status.get("last_error")
    poll_seconds = RUNTIME.poll_seconds

    stale = False
    if last_completed:
        try:
            last_dt = datetime.fromisoformat(last_completed)
            age_seconds = (datetime.now(timezone.utc) - last_dt).total_seconds()
            stale = age_seconds > poll_seconds * 4
        except (ValueError, TypeError):
            stale = True

    if last_error or stale:
        health_status = "degraded"
    else:
        health_status = "healthy"

    return JSONResponse(
        {"status": health_status, "last_error": last_error, "stale": stale},
        status_code=200 if health_status == "healthy" else 503,
    )

@app.get("/")
def index(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"title": "SigEnergy Optimizer"},
    )


@app.get("/api/status")
def api_status() -> JSONResponse:
    payload = {
        "runtime": RUNTIME.status(),
        "entities": RUNTIME.key_entities_snapshot(),
    }
    return JSONResponse(payload)


@app.get("/api/controls")
def api_controls() -> JSONResponse:
    return JSONResponse(RUNTIME.controls_snapshot())


@app.post("/api/controls/mode", dependencies=_AUTH)
async def api_control_mode(request: Request) -> JSONResponse:
    body = await request.json()
    mode = str(body.get("mode", "")).strip().lower()
    logging.getLogger(__name__).info("Human input: /api/controls/mode mode=%s", mode)
    try:
        status = await asyncio.to_thread(lambda: RUNTIME.set_control_mode(mode, source="human_api"))
        return JSONResponse({"ok": True, "runtime": status, "controls": RUNTIME.controls_snapshot()})
    except Exception as exc:
        logging.getLogger(__name__).exception("Failed setting control mode")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.post("/api/controls/tuning", dependencies=_AUTH)
async def api_control_tuning(request: Request) -> JSONResponse:
    body = await request.json()
    tuning = str(body.get("tuning", "")).strip().lower()
    logging.getLogger(__name__).info("Human input: /api/controls/tuning tuning=%s", tuning)
    try:
        result = await asyncio.to_thread(lambda: RUNTIME.set_algorithm_tuning(tuning, source="human_api"))
        return JSONResponse({"ok": True, **result, "controls": RUNTIME.controls_snapshot()})
    except Exception as exc:
        logging.getLogger(__name__).exception("Failed setting algorithm tuning")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.post("/api/controls/ess", dependencies=_AUTH)
async def api_control_ess(request: Request) -> JSONResponse:
    body = await request.json()

    def parse_float(name: str, *, min_val: float | None = 0.0, max_val: float | None = 50.0) -> float | None:
        value = body.get(name)
        if value in (None, ""):
            return None
        try:
            v = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid number for {name}: {value}")
        return _validate_float(name, v, min_val=min_val, max_val=max_val)

    ems_mode = body.get("ems_mode")
    ha_control = body.get("ha_control")
    if ha_control is not None:
        ha_control = bool(ha_control)
    logging.getLogger(__name__).info(
        "Human input: /api/controls/ess ha_control=%s mode=%s export=%s import=%s pv_max=%s",
        ha_control,
        ems_mode,
        body.get("export_limit"),
        body.get("import_limit"),
        body.get("pv_max_power_limit"),
    )

    try:
        ems_mode_str = str(ems_mode).strip() if ems_mode else None
        export_limit_val = parse_float("export_limit")
        import_limit_val = parse_float("import_limit")
        pv_max_val = parse_float("pv_max_power_limit")
        controls = await asyncio.to_thread(lambda: RUNTIME.apply_ess_controls(
            ems_mode=ems_mode_str,
            ha_control=ha_control,
            export_limit=export_limit_val,
            import_limit=import_limit_val,
            pv_max_power_limit=pv_max_val,
            source="human_api",
        ))
        return JSONResponse({
            "ok": True,
            "controls": controls,
            "warnings": controls.get("warnings", []),
        })
    except Exception as exc:
        logging.getLogger(__name__).exception("Failed applying ESS controls")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.get("/api/config")
def api_config() -> JSONResponse:
    return JSONResponse(RUNTIME.public_config())


@app.get("/api/config/raw")
def api_config_raw() -> JSONResponse:
    try:
        return JSONResponse({"ok": True, **_load_raw_config()})
    except Exception as exc:
        logging.getLogger(__name__).exception("Failed reading raw config")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)


@app.post("/api/config/validate", dependencies=_AUTH)
async def api_config_validate(request: Request) -> JSONResponse:
    body = await request.json()
    content = body.get("content")
    if not isinstance(content, str):
        return JSONResponse({"ok": False, "error": "content must be a string"}, status_code=400)
    try:
        result = await asyncio.to_thread(lambda: _validate_config_content(content))
        return JSONResponse(result)
    except Exception as exc:
        logging.getLogger(__name__).exception("Config validation failed")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.post("/api/config/save", dependencies=_AUTH)
async def api_config_save(request: Request) -> JSONResponse:
    body = await request.json()
    content = body.get("content")
    if not isinstance(content, str):
        return JSONResponse({"ok": False, "error": "content must be a string"}, status_code=400)
    logging.getLogger(__name__).info("Human input: /api/config/save")
    try:
        await asyncio.to_thread(lambda: _validate_config_content(content))
        file_info = await asyncio.to_thread(lambda: _write_config_atomic(content))
        restart_required = False
        warning = ""
        try:
            reload_result = await asyncio.to_thread(lambda: RUNTIME.reload_config_from_disk(source="human_api"))
            _refresh_key_entity_map()
        except ValueError as exc:
            reload_result = None
            restart_required = True
            warning = str(exc)
        return JSONResponse({
            "ok": True,
            "saved": True,
            "restart_required": restart_required,
            "warning": warning,
            "file": file_info,
            "config": RUNTIME.public_config(),
            "runtime": RUNTIME.status(),
            "reload": reload_result,
        })
    except Exception as exc:
        logging.getLogger(__name__).exception("Config save failed")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.get("/api/prices")
def api_prices() -> JSONResponse:
    return JSONResponse(RUNTIME.prices_snapshot())


@app.get("/api/power")
def api_power() -> JSONResponse:
    return JSONResponse(RUNTIME.power_snapshot())


@app.get("/api/price-tracking")
def api_price_tracking(date: str | None = None, limit: int = 2000) -> JSONResponse:
    """Raw price-tracking events. ?date=YYYY-MM-DD filters to that day; ?limit=N caps rows."""
    if limit < 1 or limit > 20000:
        return JSONResponse({"ok": False, "error": "limit must be between 1 and 20000"}, status_code=400)
    return JSONResponse(RUNTIME.price_tracking_events(date=date, limit=limit))


@app.get("/api/daily-earnings")
def api_daily_earnings(date: str | None = None) -> JSONResponse:
    """Per-5-min-block and daily totals for grid import cost and export revenue.
    ?date=YYYY-MM-DD selects the day; defaults to today."""
    return JSONResponse(RUNTIME.daily_earnings_summary(date=date))


@app.post("/api/import-ha-history", dependencies=_AUTH)
async def api_import_ha_history() -> JSONResponse:
    """Backfill price_tracking from HA history for the last 7 days."""
    from datetime import timedelta
    now = datetime.now(RUNTIME.tz)
    dates = [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(6, -1, -1)]
    logging.getLogger(__name__).info("Human input: /api/import-ha-history dates=%s", dates)
    try:
        results = []
        for date in dates:
            results.append(await asyncio.to_thread(lambda d=date: RUNTIME.import_ha_history(d)))
        total_inserted = sum(r["rows_inserted"] for r in results)
        total_cleared = sum(r["rows_cleared"] for r in results)
        return JSONResponse({"ok": True, "dates": dates, "rows_inserted": total_inserted, "rows_cleared": total_cleared, "detail": results})
    except Exception as exc:
        logging.getLogger(__name__).exception("import_ha_history failed")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.get("/api/earnings-history")
def api_earnings_history(days: int = 7) -> JSONResponse:
    """Daily earnings totals for the last N days (default 7)."""
    if days < 1 or days > 30:
        return JSONResponse({"ok": False, "error": "days must be between 1 and 30"}, status_code=400)
    return JSONResponse(RUNTIME.earnings_history(days=days))


@app.post("/api/simulate", dependencies=_AUTH)
async def api_simulate(request: Request) -> JSONResponse:
    try:
        logging.getLogger(__name__).info("Human input: /api/simulate")
        body = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
        passes = body.get("passes", 8) if isinstance(body, dict) else 8
        comparison = await asyncio.to_thread(lambda: RUNTIME.simulate_tuning_comparison(passes=passes, context="api"))
        return JSONResponse({"ok": True, "comparison": comparison})
    except Exception as exc:
        logging.getLogger(__name__).exception("Simulation failed")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)


@app.get("/api/logs")
def api_logs() -> JSONResponse:
    return JSONResponse({"logs": LOG_HANDLER.get_logs()})


@app.post("/api/controls/thresholds", dependencies=_AUTH)
async def api_control_thresholds(request: Request) -> JSONResponse:
    body = await request.json()
    allowed = {
        "export_threshold_low", "export_threshold_medium", "export_threshold_high",
        "export_limit_low", "export_limit_medium", "export_limit_high",
        "import_threshold_low", "import_threshold_medium", "import_threshold_high",
        "import_limit_low", "import_limit_medium", "import_limit_high",
        "ess_first_discharge_pv_threshold_kw",
    }
    _limit_keys = {
        "export_limit_low", "export_limit_medium", "export_limit_high",
        "import_limit_low", "import_limit_medium", "import_limit_high",
        "ess_first_discharge_pv_threshold_kw",
    }
    params: dict = {}
    for key in allowed:
        if key in body:
            try:
                v = float(body[key])
                _validate_float(key, v, min_val=0.0 if key in _limit_keys else None, max_val=50.0 if key in _limit_keys else None)
                params[key] = v
            except (TypeError, ValueError) as exc:
                return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    if not params:
        return JSONResponse({"ok": False, "error": "No recognised threshold fields provided"}, status_code=400)
    logging.getLogger(__name__).info("Human input: /api/controls/thresholds %s", params)
    try:
        result = await asyncio.to_thread(lambda: RUNTIME.update_thresholds(params, source="human_api"))
        return JSONResponse({"ok": True, **result})
    except Exception as exc:
        logging.getLogger(__name__).exception("Failed updating thresholds")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)


@app.post("/api/force-cycle", dependencies=_AUTH)
def api_force_cycle() -> JSONResponse:
    try:
        logging.getLogger(__name__).info("Human input: /api/force-cycle")
        status = RUNTIME.force_cycle(source="human_api")
        return JSONResponse({"ok": True, "runtime": status})
    except Exception as exc:
        logging.getLogger(__name__).exception("Manual cycle failed")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
