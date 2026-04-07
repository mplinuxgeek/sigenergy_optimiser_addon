from __future__ import annotations

from optimizer.controller._helpers import (
    _to_float, _state_float, _is_on, _attr, _bounded_number_value, _parse_ts, Decision
)
from optimizer.controller._forecasting import _ForecastMixin
from optimizer.controller._windows import _WindowsMixin
from optimizer.controller._compute import _ComputeMixin
from optimizer.controller._apply import _ApplyMixin
from optimizer.config import AppConfig
from optimizer.ha_client import HAClient
from zoneinfo import ZoneInfo


class Optimizer(_ForecastMixin, _WindowsMixin, _ComputeMixin, _ApplyMixin):
    def __init__(self, config: AppConfig, client: HAClient, timezone: str = "Australia/Adelaide") -> None:
        self.cfg = config
        self.ha = client
        self.tz = ZoneInfo(timezone)
        self.last_daily_date: str | None = None
        self.last_morning_date: str | None = None
        self._export_hysteresis_on: bool = False


__all__ = ["Decision", "Optimizer"]
