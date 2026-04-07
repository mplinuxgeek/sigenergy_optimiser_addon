# Part of Optimizer — see optimizer/controller/__init__.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from optimizer.ha_client import EntityState

LOG = logging.getLogger(__name__)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, "unknown", "unavailable", "none", ""):
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


def _state_float(states: dict[str, EntityState], entity_id: str, default: float = 0.0) -> float:
    s = states.get(entity_id)
    return _to_float(s.state if s else None, default)


def _is_on(states: dict[str, EntityState], entity_id: str) -> bool:
    s = states.get(entity_id)
    return bool(s and s.state == "on")


def _attr(states: dict[str, EntityState], entity_id: str, key: str, default: Any = None) -> Any:
    s = states.get(entity_id)
    if not s:
        return default
    return s.attributes.get(key, default)


def _bounded_number_value(states: dict[str, EntityState], entity_id: str, value: float) -> float:
    min_v = _to_float(_attr(states, entity_id, "min", None), float("-inf"))
    max_v = _to_float(_attr(states, entity_id, "max", None), float("inf"))
    bounded = value
    if bounded < min_v:
        bounded = min_v
    if bounded > max_v:
        bounded = max_v
    return bounded


def _parse_ts(value: Any, tz: ZoneInfo) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(tz)
    if not isinstance(value, str):
        return None
    text = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).astimezone(tz)
    except ValueError:
        return None


@dataclass
class Decision:
    reason: str
    desired_mode: str
    desired_export_limit: float
    desired_import_limit: float
    desired_pv_max_power_limit: float
    sunrise_soc_required: float
    battery_soc: float
    feedin_price: float
    current_price: float
    effective_ha_control: bool
    export_floor_soc: float = 0.0
    morning_dump_active: bool = False
