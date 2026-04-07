# Part of Optimizer — see optimizer/controller/__init__.py
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from optimizer.ha_client import EntityState
from optimizer.controller._helpers import _to_float, _parse_ts

LOG = logging.getLogger(__name__)


class _ForecastMixin:
    def _forecast_pv_points_kw(self, states: dict[str, EntityState], entity_id: str) -> list[tuple[datetime, float]]:
        item = states.get(entity_id)
        if not item:
            return []
        attrs = item.attributes or {}
        rows: Any = attrs.get("detailedForecast") or attrs.get("detailedHourly") or attrs.get("forecast") or attrs.get("forecasts") or []
        if not isinstance(rows, list):
            return []

        out: list[tuple[datetime, float]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            ts = _parse_ts(row.get("period_start") or row.get("time") or row.get("start_time"), self.tz)
            if not ts:
                continue
            v = _to_float(
                row.get("pv_estimate")
                if row.get("pv_estimate") is not None
                else row.get("estimate")
                if row.get("estimate") is not None
                else row.get("value"),
                -1.0,
            )
            if v < 0:
                continue
            out.append((ts, max(0.0, v)))
        out.sort(key=lambda x: x[0])
        return out

    def _forecast_price_points(
        self,
        states: dict[str, EntityState],
        entity_ids: list[str],
    ) -> list[tuple[datetime, float]]:
        out: list[tuple[datetime, float]] = []
        seen: set[tuple[str, float]] = set()
        for entity_id in entity_ids:
            if not entity_id:
                continue
            item = states.get(entity_id)
            if not item:
                continue
            attrs = item.attributes or {}
            rows: Any = attrs.get("forecast") or attrs.get("forecasts") or []
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                ts = _parse_ts(row.get("time") or row.get("start_time") or row.get("period_start"), self.tz)
                if not ts:
                    continue
                price = _to_float(
                    row.get("value")
                    if row.get("value") is not None
                    else row.get("per_kwh")
                    if row.get("per_kwh") is not None
                    else row.get("estimate"),
                    float("nan"),
                )
                if price != price:
                    continue
                k = (ts.isoformat(), float(price))
                if k in seen:
                    continue
                seen.add(k)
                out.append((ts, float(price)))
        out.sort(key=lambda x: x[0])
        return out

    def _to_local_naive(self, ts: datetime) -> datetime:
        return ts.astimezone(self.tz).replace(tzinfo=None)

    def _to_local_aware(self, ts: Any) -> datetime | None:
        if ts is None:
            return None
        if hasattr(ts, "to_pydatetime"):
            ts = ts.to_pydatetime()
        if not isinstance(ts, datetime):
            return None
        if ts.tzinfo is None:
            return ts.replace(tzinfo=self.tz)
        return ts.astimezone(self.tz)

    def _build_price_table(
        self,
        states: dict[str, EntityState],
        *,
        now: datetime,
    ) -> list[dict[str, Any]]:
        e = self.cfg.entities

        def _series(entity_ids: list[str], current_entity_id: str) -> dict[datetime, float]:
            out: dict[datetime, float] = {}
            for ts, value in self._forecast_price_points(states, entity_ids):
                local_ts = self._to_local_naive(ts)
                if local_ts.date() != now.date():
                    continue
                out[local_ts] = float(value)
            current_item = states.get(current_entity_id)
            current_value = _to_float(current_item.state if current_item else None, float("nan"))
            if current_value == current_value:
                out[self._to_local_naive(now)] = float(current_value)
            return out

        fit_points = _series([e.feedin_forecast_sensor, e.feedin_sensor], e.feedin_sensor)
        if not fit_points:
            return []

        general_points = _series([e.price_forecast_sensor, e.price_sensor], e.price_sensor)
        all_times = sorted(set(fit_points) | set(general_points))
        fit_item = states.get(e.feedin_sensor) or states.get(e.feedin_forecast_sensor)
        spike_status = ""
        if fit_item:
            spike_status = str(fit_item.attributes.get("spike_status", "") or "").strip().lower()
        rows: list[dict[str, Any]] = []
        for ts in all_times:
            row: dict[str, Any] = {
                "time": ts,
                "fit": fit_points.get(ts, float("nan")),
            }
            general_value = general_points.get(ts)
            if general_value is not None:
                row["general"] = general_value
            if spike_status:
                row["spike_status"] = spike_status
            rows.append(row)
        return rows
