# Part of OptimizerRuntime — see optimizer/runtime/__init__.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

LOG = logging.getLogger(__name__)


class _PriceDataMixin:
    @staticmethod
    def _to_float(value: Any) -> float | None:
        try:
            if value in (None, "", "unknown", "unavailable", "none"):
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_iso_ts(value: Any) -> datetime | None:
        if not value or not isinstance(value, str):
            return None
        text = value.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
        if dt.tzinfo is None:
            return None
        return dt

    def _extract_price_points(self, item: Any, *, tz: ZoneInfo, day_start: datetime, day_end: datetime) -> list[dict[str, Any]]:
        if not item:
            return []
        attrs = item.attributes or {}
        raw = attrs.get("forecast") or attrs.get("forecasts") or []
        out: list[dict[str, Any]] = []
        for row in raw:
            if not isinstance(row, dict):
                continue
            ts = self._parse_iso_ts(row.get("time") or row.get("start_time") or row.get("nem_time"))
            if not ts:
                continue
            local_ts = ts.astimezone(tz)
            if local_ts < day_start or local_ts >= day_end:
                continue
            value = self._to_float(row.get("value"))
            if value is None:
                value = self._to_float(row.get("per_kwh"))
            if value is None:
                continue
            out.append({"time": local_ts.isoformat(), "value": value, "kind": "forecast"})
        seen: set[str] = set()
        dedup: list[dict[str, Any]] = []
        for point in sorted(out, key=lambda p: p["time"]):
            key = point["time"]
            if key in seen:
                continue
            seen.add(key)
            dedup.append(point)
        return dedup

    def _extract_pv_forecast_points(
        self,
        item: Any,
        *,
        tz: ZoneInfo,
        day_start: datetime,
        day_end: datetime,
    ) -> list[dict[str, Any]]:
        if not item:
            return []
        attrs = item.attributes or {}
        raw = attrs.get("detailedForecast") or attrs.get("detailedHourly") or attrs.get("forecast") or []
        out: list[dict[str, Any]] = []
        for row in raw:
            if not isinstance(row, dict):
                continue
            ts = self._parse_iso_ts(row.get("period_start") or row.get("time") or row.get("start_time"))
            if not ts:
                continue
            local_ts = ts.astimezone(tz)
            if local_ts < day_start or local_ts >= day_end:
                continue
            value = self._to_float(row.get("pv_estimate"))
            if value is None:
                value = self._to_float(row.get("estimate"))
            if value is None:
                value = self._to_float(row.get("value"))
            if value is None:
                continue
            out.append({"time": local_ts.isoformat(), "value": value, "kind": "forecast"})
        dedup: dict[str, dict[str, Any]] = {}
        for point in out:
            dedup[point["time"]] = point
        return [dedup[k] for k in sorted(dedup.keys())]

    def _extract_history_points(
        self,
        rows: list[dict[str, Any]],
        *,
        tz: ZoneInfo,
        day_start: datetime,
        day_end: datetime,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            ts = self._parse_iso_ts(row.get("last_changed") or row.get("last_updated"))
            if not ts:
                continue
            local_ts = ts.astimezone(tz)
            if local_ts < day_start or local_ts >= day_end:
                continue
            value = self._to_float(row.get("state"))
            if value is None:
                continue
            out.append({"time": local_ts.isoformat(), "value": value, "kind": "history"})
        seen: set[str] = set()
        dedup: list[dict[str, Any]] = []
        for point in sorted(out, key=lambda p: p["time"]):
            key = point["time"]
            if key in seen:
                continue
            seen.add(key)
            dedup.append(point)
        return dedup

    @staticmethod
    def _merge_price_points(
        *,
        history_points: list[dict[str, Any]],
        forecast_points: list[dict[str, Any]],
        now: datetime,
    ) -> list[dict[str, Any]]:
        cutoff = now.isoformat()
        merged: list[dict[str, Any]] = []
        for p in history_points:
            if p.get("time", "") <= cutoff:
                merged.append({"time": p["time"], "value": p["value"], "kind": "history"})
        for p in forecast_points:
            if p.get("time", "") > cutoff:
                merged.append({"time": p["time"], "value": p["value"], "kind": "forecast"})
        if not merged and forecast_points:
            merged = [{"time": p["time"], "value": p["value"], "kind": p.get("kind", "forecast")} for p in forecast_points]
        seen: set[str] = set()
        dedup: list[dict[str, Any]] = []
        for point in sorted(merged, key=lambda p: p["time"]):
            key = point["time"]
            if key in seen:
                continue
            seen.add(key)
            dedup.append(point)
        return dedup

    @staticmethod
    def _extend_forecast_to_day_end(points: list[dict[str, Any]], day_end: datetime) -> list[dict[str, Any]]:
        if not points:
            return points
        last = points[-1]
        if str(last.get("kind", "")) != "forecast":
            return points
        last_ts = _PriceDataMixin._parse_iso_ts(last.get("time"))
        if not last_ts:
            return points
        target = day_end.isoformat()
        if last_ts >= day_end or str(last.get("time")) == target:
            return points
        out = list(points)
        out.append({"time": target, "value": last.get("value"), "kind": "forecast"})
        return out

    def prices_snapshot(self, date: str | None = None) -> dict[str, Any]:
        states = self.client.get_all_states()
        e = self.cfg.entities
        now = datetime.now(self.tz)
        if date:
            from datetime import date as _date_type
            _d = _date_type.fromisoformat(date)
            day_start = datetime(_d.year, _d.month, _d.day, tzinfo=self.tz)
            day_end = day_start + timedelta(days=1)
            now = day_end  # treat end of historical day as "now"
            is_historical = True
        else:
            day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end = day_start + timedelta(days=1)
            is_historical = False
        sun_item = states.get(e.sun_entity)

        def _pick_forecast_item(primary_entity: str, explicit_forecast_entity: str) -> tuple[Any, str]:
            candidates: list[str] = []
            if explicit_forecast_entity:
                candidates.append(explicit_forecast_entity)
            candidates.extend([primary_entity, f"{primary_entity}_detailed"])
            seen: set[str] = set()
            for entity_id in candidates:
                if not entity_id or entity_id in seen:
                    continue
                seen.add(entity_id)
                item = states.get(entity_id)
                if not item:
                    continue
                pts = self._extract_price_points(item, tz=self.tz, day_start=day_start, day_end=day_end)
                if pts:
                    return item, entity_id
            fallback_entity = explicit_forecast_entity or primary_entity
            return states.get(fallback_entity), fallback_entity

        day_key = day_start.date().isoformat()

        def _pick_history_rows(primary_entity: str, forecast_entity: str) -> tuple[list[dict[str, Any]], str]:
            candidates = [forecast_entity, primary_entity, f"{primary_entity}_detailed"]
            seen: set[str] = set()
            for entity_id in candidates:
                if not entity_id or entity_id in seen:
                    continue
                seen.add(entity_id)
                try:
                    rows = self.client.get_history(entity_id=entity_id, start=day_start, end=now)
                except Exception as exc:
                    LOG.debug("History fetch failed for %s: %s", entity_id, exc)
                    rows = []
                if rows:
                    return rows, entity_id
            return [], (forecast_entity or primary_entity)

        def _cached_history_points(cache_key: str, points: list[dict[str, Any]], entity_id: str) -> list[dict[str, Any]]:
            if points:
                self._price_history_cache[cache_key] = {
                    "day_key": day_key,
                    "points": points,
                    "entity_id": entity_id,
                }
                return points
            cached = self._price_history_cache.get(cache_key)
            if cached and cached.get("day_key") == day_key:
                return list(cached.get("points") or [])
            return points

        import_item = states.get(e.price_sensor)
        export_item = states.get(e.feedin_sensor)
        import_forecast_item, import_forecast_entity = _pick_forecast_item(e.price_sensor, e.price_forecast_sensor)
        export_forecast_item, export_forecast_entity = _pick_forecast_item(e.feedin_sensor, e.feedin_forecast_sensor)
        import_forecast_points = self._extract_price_points(
            import_forecast_item, tz=self.tz, day_start=day_start, day_end=day_end
        )
        export_forecast_points = self._extract_price_points(
            export_forecast_item, tz=self.tz, day_start=day_start, day_end=day_end
        )
        import_history_rows, import_history_entity = _pick_history_rows(e.price_sensor, import_forecast_entity)
        export_history_rows, export_history_entity = _pick_history_rows(e.feedin_sensor, export_forecast_entity)
        import_history_points = self._extract_history_points(
            import_history_rows, tz=self.tz, day_start=day_start, day_end=day_end
        )
        export_history_points = self._extract_history_points(
            export_history_rows, tz=self.tz, day_start=day_start, day_end=day_end
        )
        import_history_points = _cached_history_points("import", import_history_points, import_history_entity)
        export_history_points = _cached_history_points("export", export_history_points, export_history_entity)
        import_points = self._merge_price_points(
            history_points=import_history_points, forecast_points=import_forecast_points, now=now
        )
        export_points = self._merge_price_points(
            history_points=export_history_points, forecast_points=export_forecast_points, now=now
        )
        import_points = self._extend_forecast_to_day_end(import_points, day_end)
        export_points = self._extend_forecast_to_day_end(export_points, day_end)
        pv_item = states.get(e.forecast_today_sensor)
        pv_points = self._extract_pv_forecast_points(pv_item, tz=self.tz, day_start=day_start, day_end=day_end)
        pv_total = self._to_float(pv_item.state if pv_item else None)

        sunrise_iso: str | None = None
        sunset_iso: str | None = None
        if sun_item:
            is_sun_up = str(sun_item.state) == "above_horizon"
            next_rising = self._parse_iso_ts((sun_item.attributes or {}).get("next_rising"))
            next_setting = self._parse_iso_ts((sun_item.attributes or {}).get("next_setting"))
            if next_rising:
                next_rising = next_rising.astimezone(self.tz)
            if next_setting:
                next_setting = next_setting.astimezone(self.tz)

            today_real = datetime.now(self.tz)
            sunrise_dt: datetime | None = None
            sunset_dt: datetime | None = None
            if next_rising and next_setting:
                if is_sun_up:
                    sunrise_dt = next_rising - timedelta(days=1)
                    sunset_dt = next_setting
                else:
                    if next_rising.date() == today_real.date():
                        sunrise_dt = next_rising
                        sunset_dt = next_setting
                    else:
                        sunrise_dt = next_rising - timedelta(days=1)
                        sunset_dt = next_setting - timedelta(days=1)

            if sunrise_dt and sunset_dt:
                if is_historical:
                    # Shift today's sun times to the historical date (±1-3 min error is fine visually)
                    delta = day_start.date() - today_real.date()
                    sunrise_dt = sunrise_dt + timedelta(days=delta.days)
                    sunset_dt = sunset_dt + timedelta(days=delta.days)
                sunrise_iso = sunrise_dt.isoformat()
                sunset_iso = sunset_dt.isoformat()

        return {
            "generated_at": now.isoformat(),
            "day_start": day_start.isoformat(),
            "day_end": day_end.isoformat(),
            "timezone": str(self.tz),
            "sun": {
                "entity_id": e.sun_entity,
                "state": sun_item.state if sun_item else "unknown",
                "sunrise": sunrise_iso,
                "sunset": sunset_iso,
            },
            "import": {
                "entity_id": e.price_sensor,
                "forecast_entity_id": import_forecast_entity,
                "history_entity_id": import_history_entity,
                "current": None if is_historical else self._to_float(import_item.state if import_item else None),
                "points": import_points,
            },
            "export": {
                "entity_id": e.feedin_sensor,
                "forecast_entity_id": export_forecast_entity,
                "history_entity_id": export_history_entity,
                "current": None if is_historical else self._to_float(export_item.state if export_item else None),
                "points": export_points,
            },
            "pv_forecast": {
                "entity_id": e.forecast_today_sensor,
                "total_kwh": pv_total,
                "points": pv_points,
            },
        }
