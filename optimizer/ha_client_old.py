from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import quote

import requests

LOG = logging.getLogger(__name__)


@dataclass
class EntityState:
    entity_id: str
    state: str
    attributes: dict[str, Any]


class HAClient:
    # Seconds to cache /api/states responses.  The cache is also invalidated
    # after any call_service write so post-write reads always see fresh state.
    _STATES_CACHE_TTL: float = 2.0

    def __init__(self, url: str, token: str, timeout: int = 20) -> None:
        self._url = url.rstrip("/")
        self._s = requests.Session()
        self._s.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )
        self._timeout = timeout
        self._states_cache: dict[str, EntityState] | None = None
        self._states_cache_ts: float = 0.0

    def _invalidate_states_cache(self) -> None:
        self._states_cache = None
        self._states_cache_ts = 0.0

    def get_all_states(self, retries: int = 2, use_cache: bool = True) -> dict[str, EntityState]:
        now = time.monotonic()
        if use_cache and self._states_cache is not None:
            if (now - self._states_cache_ts) < self._STATES_CACHE_TTL:
                return dict(self._states_cache)
        last_exc: Exception | None = None
        for attempt in range(retries + 1):
            try:
                r = self._s.get(f"{self._url}/api/states", timeout=self._timeout)
                if not r.ok:
                    raise requests.HTTPError(
                        f"{r.status_code} {r.reason} for GET /api/states: {r.text}",
                        response=r,
                    )
                out: dict[str, EntityState] = {}
                for item in r.json():
                    eid = item.get("entity_id", "")
                    if not eid:
                        continue
                    out[eid] = EntityState(
                        entity_id=eid,
                        state=str(item.get("state", "unknown")),
                        attributes=item.get("attributes") or {},
                    )
                self._states_cache = out
                self._states_cache_ts = time.monotonic()
                return dict(out)
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
                    "Transient GET /api/states failure (attempt=%d/%d): %s",
                    attempt + 1,
                    retries + 1,
                    exc,
                )
                time.sleep(0.5 * (attempt + 1))
        if last_exc:
            raise last_exc
        return {}  # unreachable

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
        r = self._s.get(f"{self._url}/api/history/period/{start_path}", params=params, timeout=self._timeout)
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
            f"{self._url}/api/services/{domain}/{service}",
            json=payload,
            timeout=self._timeout,
        )
        if not r.ok:
            raise requests.HTTPError(
                f"{r.status_code} {r.reason} for {domain}.{service}: {r.text}",
                response=r,
            )
        # Invalidate state cache: any service call may change entity states.
        self._invalidate_states_cache()

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
        # HA accepts a single entity id or a list in target data.
        self.call_service("homeassistant", "update_entity", {"entity_id": ids})

    def reload_config_entry(self, entry_id: str) -> None:
        if not entry_id:
            return
        self.call_service("homeassistant", "reload_config_entry", {"entry_id": entry_id})
