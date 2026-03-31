from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable
from urllib.parse import quote, urlparse

import requests
import websockets.sync.client

LOG = logging.getLogger(__name__)


@dataclass
class EntityState:
    entity_id: str
    state: str
    attributes: dict[str, Any]


class HAClient:
    """Home Assistant client using WebSocket for real-time state updates and REST for commands."""

    def __init__(self, url: str, token: str, timeout: int = 20) -> None:
        self._http_url = url.rstrip("/")
        self._token = token
        self._timeout = timeout

        # Convert:
        # - http://host:port -> ws://host:port/api/websocket
        # - http://supervisor/core -> ws://supervisor/core/websocket
        parsed = urlparse(self._http_url)
        ws_scheme = "wss" if parsed.scheme == "https" else "ws"
        ws_base_path = parsed.path.rstrip("/")
        if ws_base_path:
            self._ws_url = f"{ws_scheme}://{parsed.netloc}{ws_base_path}/websocket"
        else:
            self._ws_url = f"{ws_scheme}://{parsed.netloc}/api/websocket"

        # REST session for service calls
        self._s = requests.Session()
        self._s.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )

        # State cache updated in real-time by WebSocket events
        self._lock = threading.Lock()
        self._states_cache: dict[str, EntityState] = {}
        self._cache_ready = threading.Event()

        # State-change listeners (called after cache update, outside _lock)
        self._state_listeners: list[Callable[[str, str, dict[str, Any]], None]] = []
        self._state_listeners_lock = threading.Lock()

        # WebSocket background thread
        self._ws: websockets.sync.client.ClientConnection | None = None
        self._ws_thread: threading.Thread | None = None
        self._ws_stop = threading.Event()
        self._msg_id = 0

        # Start WebSocket connection in background
        self._start_websocket()

    def _start_websocket(self) -> None:
        if self._ws_thread and self._ws_thread.is_alive():
            return
        self._ws_stop.clear()
        self._ws_thread = threading.Thread(target=self._ws_loop, daemon=True, name="ha-websocket")
        self._ws_thread.start()

    def _ws_loop(self) -> None:
        """Background thread that maintains WebSocket connection and processes events."""
        backoff = 1.0
        while not self._ws_stop.is_set():
            try:
                LOG.info("Connecting to Home Assistant WebSocket at %s", self._ws_url)
                with websockets.sync.client.connect(
                    self._ws_url,
                    open_timeout=10,
                    close_timeout=5,
                    max_size=2**23,  # 8 MB
                ) as ws:
                    self._ws = ws
                    backoff = 1.0  # Reset backoff on successful connection
                    self._ws_session(ws)
            except Exception as exc:
                LOG.warning("WebSocket connection failed, reconnecting in %.1fs: %s", backoff, exc)
                self._ws = None
                self._ws_stop.wait(backoff)
                backoff = min(backoff * 1.5, 30.0)

    def _ws_session(self, ws: websockets.sync.client.ClientConnection) -> None:
        """Handle a single WebSocket session: auth, subscribe, listen."""
        # 1. Receive auth_required
        msg = json.loads(ws.recv(timeout=10))
        if msg.get("type") != "auth_required":
            raise RuntimeError(f"Expected auth_required, got {msg.get('type')}")

        # 2. Send auth
        ws.send(json.dumps({"type": "auth", "access_token": self._token}))

        # 3. Receive auth_ok or auth_invalid
        msg = json.loads(ws.recv(timeout=10))
        if msg.get("type") == "auth_invalid":
            raise RuntimeError(f"WebSocket auth failed: {msg.get('message')}")
        if msg.get("type") != "auth_ok":
            raise RuntimeError(f"Expected auth_ok, got {msg.get('type')}")

        LOG.info("Home Assistant WebSocket authenticated")

        # 4. Fetch initial state snapshot via REST (faster than subscribing and waiting)
        self._fetch_initial_states()

        # 5. Subscribe to state_changed events
        self._msg_id += 1
        sub_id = self._msg_id
        ws.send(json.dumps({
            "id": sub_id,
            "type": "subscribe_events",
            "event_type": "state_changed",
        }))
        LOG.info("Subscribed to state_changed events (id=%d)", sub_id)

        # 6. Listen for messages
        while not self._ws_stop.is_set():
            try:
                raw = ws.recv(timeout=1.0)
                msg = json.loads(raw)
                self._handle_message(msg)
            except TimeoutError:
                # recv timeout allows us to check _ws_stop periodically
                continue
            except Exception as exc:
                LOG.warning("WebSocket recv error, reconnecting: %s", exc)
                raise

    def _fetch_initial_states(self) -> None:
        """Fetch all states via REST to seed the cache before events start flowing."""
        try:
            r = self._s.get(f"{self._http_url}/api/states", timeout=self._timeout)
            if not r.ok:
                LOG.warning("Failed to fetch initial states: %s %s", r.status_code, r.reason)
                return
            with self._lock:
                self._states_cache.clear()
                for item in r.json():
                    eid = item.get("entity_id", "")
                    if not eid:
                        continue
                    self._states_cache[eid] = EntityState(
                        entity_id=eid,
                        state=str(item.get("state", "unknown")),
                        attributes=item.get("attributes") or {},
                    )
                self._cache_ready.set()
            LOG.info("Loaded %d initial entity states", len(self._states_cache))
        except Exception as exc:
            LOG.warning("Failed to fetch initial states: %s", exc)

    def add_state_listener(self, callback: Callable[[str, str, dict[str, Any]], None]) -> None:
        """Register a callback invoked on every HA state_changed event.

        The callback receives (entity_id, new_state_str, attributes) and is
        called from the WebSocket background thread, outside the cache lock.
        """
        with self._state_listeners_lock:
            self._state_listeners.append(callback)

    def _handle_message(self, msg: dict[str, Any]) -> None:
        """Process incoming WebSocket messages."""
        msg_type = msg.get("type")
        if msg_type == "event":
            event = msg.get("event") or {}
            event_type = event.get("event_type")
            if event_type == "state_changed":
                data = event.get("data") or {}
                new_state = data.get("new_state")
                if new_state:
                    eid = new_state.get("entity_id", "")
                    if eid:
                        entity_state = EntityState(
                            entity_id=eid,
                            state=str(new_state.get("state", "unknown")),
                            attributes=new_state.get("attributes") or {},
                        )
                        with self._lock:
                            self._states_cache[eid] = entity_state
                        # Fire listeners outside the lock to avoid deadlocks
                        with self._state_listeners_lock:
                            listeners = list(self._state_listeners)
                        for cb in listeners:
                            try:
                                cb(eid, entity_state.state, entity_state.attributes)
                            except Exception:
                                pass
        elif msg_type == "result":
            # Response to a command (like subscribe_events)
            if not msg.get("success"):
                LOG.warning("WebSocket command failed: %s", msg)

    def get_all_states(self, retries: int = 2, use_cache: bool = True) -> dict[str, EntityState]:
        """Return current state snapshot from the WebSocket-maintained cache."""
        # Wait up to 10 seconds for cache to be ready
        if not self._cache_ready.wait(timeout=10):
            LOG.warning("State cache not ready after 10s, returning empty dict")
            return {}
        with self._lock:
            return dict(self._states_cache)

    def stop(self) -> None:
        """Stop the WebSocket background thread."""
        self._ws_stop.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        if self._ws_thread:
            self._ws_thread.join(timeout=2)

    # ── REST-based service calls (unchanged from original) ──────────────────

    def get_history(
        self,
        *,
        entity_id: str,
        start: datetime,
        end: datetime,
        minimal_response: bool = True,
    ) -> list[dict[str, Any]]:
        if not entity_id:
            return []
        start_path = quote(start.isoformat(), safe="")
        params = {
            "filter_entity_id": entity_id,
            "end_time": end.isoformat(),
            "minimal_response": "1" if minimal_response else "0",
            "no_attributes": "1",
            "significant_changes_only": "0",
        }
        r = self._s.get(f"{self._http_url}/api/history/period/{start_path}", params=params, timeout=self._timeout)
        if not r.ok:
            raise requests.HTTPError(
                f"{r.status_code} {r.reason} for GET /api/history/period: {r.text}",
                response=r,
            )
        payload = r.json()
        if not isinstance(payload, list) or not payload:
            return []
        rows = payload[0]
        if not isinstance(rows, list):
            return []
        out: list[dict[str, Any]] = []
        for item in rows:
            if isinstance(item, dict):
                out.append(item)
        return out

    def call_service(self, domain: str, service: str, data: dict[str, Any] | None = None) -> None:
        payload = data or {}
        LOG.debug("call_service: %s.%s %s", domain, service, payload)
        r = self._s.post(
            f"{self._http_url}/api/services/{domain}/{service}",
            json=payload,
            timeout=self._timeout,
        )
        if not r.ok:
            raise requests.HTTPError(
                f"{r.status_code} {r.reason} for {domain}.{service}: {r.text}",
                response=r,
            )

    def _call_service_with_retries(
        self,
        *,
        domain: str,
        service: str,
        payload: dict[str, Any],
        action_label: str,
        retries: int = 2,
        delay: float = 0.12,
    ) -> None:
        last_exc: Exception | None = None
        for attempt in range(retries + 1):
            try:
                self.call_service(domain, service, payload)
                return
            except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as exc:
                last_exc = exc
                if isinstance(exc, requests.HTTPError):
                    status = exc.response.status_code if exc.response is not None else None
                    if status is not None and status < 500 and attempt >= retries:
                        raise
                    if status is not None and status < 500:
                        raise
                if attempt >= retries:
                    raise
                LOG.warning(
                    "Transient %s failure (attempt=%s/%s): %s",
                    action_label,
                    attempt + 1,
                    retries + 1,
                    exc,
                )
                time.sleep(delay * (attempt + 1))
        if last_exc:
            raise last_exc

    def set_number(self, entity_id: str, value: float) -> None:
        payload = {"entity_id": entity_id, "value": float(value)}
        self._call_service_with_retries(
            domain="number",
            service="set_value",
            payload=payload,
            action_label=f"number.set_value (entity={entity_id}, value={payload['value']})",
        )

    def set_input_number(self, entity_id: str, value: float) -> None:
        self.call_service("input_number", "set_value", {"entity_id": entity_id, "value": float(value)})

    def set_input_text(self, entity_id: str, value: str) -> None:
        self.call_service("input_text", "set_value", {"entity_id": entity_id, "value": str(value)})

    def set_select(self, entity_id: str, option: str) -> None:
        payload = {"entity_id": entity_id, "option": option}
        self._call_service_with_retries(
            domain="select",
            service="select_option",
            payload=payload,
            action_label=f"select.select_option (entity={entity_id}, option={option})",
        )

    def switch_on(self, entity_id: str) -> None:
        payload = {"entity_id": entity_id}
        self._call_service_with_retries(
            domain="switch",
            service="turn_on",
            payload=payload,
            action_label=f"switch.turn_on (entity={entity_id})",
        )

    def switch_off(self, entity_id: str) -> None:
        payload = {"entity_id": entity_id}
        self._call_service_with_retries(
            domain="switch",
            service="turn_off",
            payload=payload,
            action_label=f"switch.turn_off (entity={entity_id})",
        )

    def bool_on(self, entity_id: str) -> None:
        payload = {"entity_id": entity_id}
        self._call_service_with_retries(
            domain="input_boolean",
            service="turn_on",
            payload=payload,
            action_label=f"input_boolean.turn_on (entity={entity_id})",
        )

    def bool_off(self, entity_id: str) -> None:
        payload = {"entity_id": entity_id}
        self._call_service_with_retries(
            domain="input_boolean",
            service="turn_off",
            payload=payload,
            action_label=f"input_boolean.turn_off (entity={entity_id})",
        )

    def logbook(self, name: str, message: str, entity_id: str | None = None) -> None:
        payload: dict[str, Any] = {"name": name, "message": message}
        if entity_id:
            payload["entity_id"] = entity_id
        self.call_service("logbook", "log", payload)

    def notify(self, service_name: str, title: str, message: str) -> None:
        if not service_name:
            return
        if "." not in service_name:
            raise ValueError(f"notification_service must be domain.service, got {service_name}")
        domain, service = service_name.split(".", 1)
        self.call_service(domain, service, {"title": title, "message": message})

    def automation_on(self, entity_id: str) -> None:
        payload = {"entity_id": entity_id}
        self._call_service_with_retries(
            domain="automation",
            service="turn_on",
            payload=payload,
            action_label=f"automation.turn_on (entity={entity_id})",
        )

    def automation_off(self, entity_id: str, *, stop_actions: bool = True) -> None:
        payload = {"entity_id": entity_id, "stop_actions": bool(stop_actions)}
        self._call_service_with_retries(
            domain="automation",
            service="turn_off",
            payload=payload,
            action_label=f"automation.turn_off (entity={entity_id}, stop_actions={bool(stop_actions)})",
        )

    def set_input_select(self, entity_id: str, option: str) -> None:
        payload = {"entity_id": entity_id, "option": option}
        self._call_service_with_retries(
            domain="input_select",
            service="select_option",
            payload=payload,
            action_label=f"input_select.select_option (entity={entity_id}, option={option})",
        )

    def update_entity(self, entity_id: str) -> None:
        if not entity_id:
            return
        self.call_service("homeassistant", "update_entity", {"entity_id": entity_id})

    def update_entities(self, entity_ids: list[str]) -> None:
        ids = [eid for eid in entity_ids if eid]
        if not ids:
            return
        self.call_service("homeassistant", "update_entity", {"entity_id": ids})

    def reload_config_entry(self, entry_id: str) -> None:
        if not entry_id:
            return
        self.call_service("homeassistant", "reload_config_entry", {"entry_id": entry_id})
