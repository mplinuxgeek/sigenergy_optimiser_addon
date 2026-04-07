# Part of OptimizerRuntime — see optimizer/runtime/__init__.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

LOG = logging.getLogger(__name__)


class _PowerDataMixin:
    def _extract_history_series(
        self,
        *,
        entity_id: str,
        day_start: datetime,
        now: datetime,
        to_kw: bool = False,
        floor_zero: bool = False,
        fill_flat_if_empty: bool = False,
    ) -> list[dict[str, Any]]:
        if not entity_id:
            return []
        try:
            rows = self.client.get_history(entity_id=entity_id, start=day_start, end=now)
        except Exception as exc:
            LOG.debug("History series fetch failed for %s: %s", entity_id, exc)
            rows = []
        points: list[dict[str, Any]] = []
        for row in rows:
            ts = self._parse_iso_ts(row.get("last_changed") or row.get("last_updated"))
            if not ts:
                continue
            local_ts = ts.astimezone(self.tz)
            if local_ts < day_start or local_ts > now:
                continue
            val = self._to_float(row.get("state"))
            if val is None:
                continue
            if to_kw and abs(val) > 1000:
                val = val / 1000.0
            if floor_zero and val < 0:
                val = 0.0
            points.append({"time": local_ts.isoformat(), "value": val, "kind": "history"})

        points.sort(key=lambda p: p["time"])
        dedup: dict[str, dict[str, Any]] = {}
        for p in points:
            dedup[p["time"]] = p
        out = [dedup[k] for k in sorted(dedup.keys())]

        if fill_flat_if_empty and not out:
            states = self.client.get_all_states()
            item = states.get(entity_id)
            cur = self._to_float(item.state if item else None)
            if cur is not None:
                if to_kw and abs(cur) > 1000:
                    cur = cur / 1000.0
                if floor_zero and cur < 0:
                    cur = 0.0
                out = [
                    {"time": day_start.isoformat(), "value": cur, "kind": "history"},
                    {"time": now.isoformat(), "value": cur, "kind": "history"},
                ]
        return out

    def _pv_forecast_kw_points(self, pv_points: list[dict[str, Any]]) -> list[dict[str, Any]]:
        # Solcast detailedForecast `pv_estimate` values are already interval power-style values.
        # Keep them unchanged to avoid doubling on 30-minute intervals.
        out: list[dict[str, Any]] = []
        for p in pv_points:
            ts = self._parse_iso_ts(p.get("time"))
            val = self._to_float(p.get("value"))
            if not ts or val is None:
                continue
            out.append({"time": ts.astimezone(self.tz).isoformat(), "value": max(0.0, val), "kind": "forecast"})
        return out

    @staticmethod
    def _downsample_points(points: list[dict[str, Any]], max_points: int = 1200) -> list[dict[str, Any]]:
        if len(points) <= max_points:
            return points
        if max_points < 3:
            return [points[0], points[-1]]

        # Preserve curve shape by keeping local minima/maxima per bucket,
        # rather than fixed-stride sampling which can flatten spikes/troughs.
        first = points[0]
        last = points[-1]
        middle = points[1:-1]
        if not middle:
            return [first, last]

        slots = max_points - 2
        bucket_count = max(1, slots // 2)
        chunk_size = max(1, (len(middle) + bucket_count - 1) // bucket_count)

        selected: list[dict[str, Any]] = []
        for start in range(0, len(middle), chunk_size):
            chunk = middle[start : start + chunk_size]
            if not chunk:
                continue
            if len(chunk) == 1:
                selected.append(chunk[0])
                continue

            min_idx = 0
            max_idx = 0
            min_val = float(chunk[0].get("value", 0.0))
            max_val = min_val
            for i in range(1, len(chunk)):
                v = float(chunk[i].get("value", 0.0))
                if v < min_val:
                    min_val = v
                    min_idx = i
                if v > max_val:
                    max_val = v
                    max_idx = i

            if min_idx == max_idx:
                selected.append(chunk[min_idx])
            elif min_idx < max_idx:
                selected.extend([chunk[min_idx], chunk[max_idx]])
            else:
                selected.extend([chunk[max_idx], chunk[min_idx]])

        if len(selected) > slots:
            stride = max(1, len(selected) // slots)
            reduced = [selected[i] for i in range(0, len(selected), stride)]
            selected = reduced[:slots]

        out = [first, *selected, last]
        out.sort(key=lambda p: p.get("time", ""))
        return out

    def power_snapshot(self, date: str | None = None) -> dict[str, Any]:
        e = self.cfg.entities
        now = datetime.now(self.tz)
        if date:
            from datetime import date as _date_type
            _d = _date_type.fromisoformat(date)
            day_start = datetime(_d.year, _d.month, _d.day, tzinfo=self.tz)
            day_end = day_start + timedelta(days=1)
            now = day_end
            is_historical = True
        else:
            day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end = day_start + timedelta(days=1)
            is_historical = False

        pv_forecast_kwh = self._extract_pv_forecast_points(
            self.client.get_all_states().get(e.forecast_today_sensor),
            tz=self.tz,
            day_start=day_start,
            day_end=day_end,
        )
        pv_forecast_kw = self._pv_forecast_kw_points(pv_forecast_kwh)

        series = {
            "battery_soc": {
                "entity_id": e.battery_soc_sensor,
                "unit": "%",
                "axis": "right",
                "points": self._downsample_points(self._extract_history_series(
                    entity_id=e.battery_soc_sensor,
                    day_start=day_start,
                    now=now,
                    to_kw=False,
                    floor_zero=True,
                    fill_flat_if_empty=not is_historical,
                )),
            },
            "min_soc": {
                "entity_id": e.min_soc_to_sunrise_helper,
                "unit": "%",
                "axis": "right",
                "points": self._downsample_points(self._extract_history_series(
                    entity_id=e.min_soc_to_sunrise_helper,
                    day_start=day_start,
                    now=now,
                    to_kw=False,
                    floor_zero=True,
                    fill_flat_if_empty=not is_historical,
                )),
            },
            "pv_output": {
                "entity_id": e.pv_power_sensor,
                "unit": "kW",
                "axis": "left",
                "points": self._downsample_points(self._extract_history_series(
                    entity_id=e.pv_power_sensor,
                    day_start=day_start,
                    now=now,
                    to_kw=True,
                    floor_zero=True,
                )),
            },
            "pv_forecast_kw": {
                "entity_id": e.forecast_today_sensor,
                "unit": "kW",
                "axis": "left",
                "points": self._downsample_points(pv_forecast_kw),
            },
            "grid_import_power": {
                "entity_id": e.grid_import_power_sensor,
                "unit": "kW",
                "axis": "left",
                "points": self._downsample_points(self._extract_history_series(
                    entity_id=e.grid_import_power_sensor,
                    day_start=day_start,
                    now=now,
                    to_kw=True,
                    floor_zero=True,
                )),
            },
            "grid_export_power": {
                "entity_id": e.grid_export_power_sensor,
                "unit": "kW",
                "axis": "left",
                "points": self._downsample_points(self._extract_history_series(
                    entity_id=e.grid_export_power_sensor,
                    day_start=day_start,
                    now=now,
                    to_kw=True,
                    floor_zero=True,
                )),
            },
            "consumption_power": {
                "entity_id": e.consumed_power_sensor,
                "unit": "kW",
                "axis": "left",
                "points": self._downsample_points(self._extract_history_series(
                    entity_id=e.consumed_power_sensor,
                    day_start=day_start,
                    now=now,
                    to_kw=True,
                    floor_zero=True,
                )),
            },
        }

        return {
            "generated_at": now.isoformat(),
            "day_start": day_start.isoformat(),
            "day_end": day_end.isoformat(),
            "timezone": str(self.tz),
            "series": series,
            "sun": self.prices_snapshot(date=date).get("sun", {}),
        }
