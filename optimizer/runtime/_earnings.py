# Part of OptimizerRuntime — see optimizer/runtime/__init__.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

LOG = logging.getLogger(__name__)


class _EarningsMixin:
    def price_tracking_events(self, date: str | None = None, limit: int = 2000) -> dict[str, Any]:
        """Return raw price tracking events for a given date (YYYY-MM-DD) or the most recent records."""
        events = self.state_store.get_price_events(date=date, limit=limit)
        return {
            "date": date,
            "count": len(events),
            "events": events,
        }

    def daily_earnings_summary(self, date: str | None = None) -> dict[str, Any]:
        """Return per-block and totals for cost/revenue for a given date (YYYY-MM-DD).
        Defaults to today in the configured timezone."""
        if not date:
            date = datetime.now(self.tz).strftime("%Y-%m-%d")
        return self.state_store.daily_earnings_summary(date)

    def earnings_history(self, days: int = 7) -> dict[str, Any]:
        """Return daily earnings summaries (totals only, no block detail) for the last N days."""
        now = datetime.now(self.tz)
        summaries = []
        for i in range(days - 1, -1, -1):
            date = (now - timedelta(days=i)).strftime("%Y-%m-%d")
            day_summary = self.state_store.daily_earnings_summary(date)
            summaries.append({
                "date": date,
                "import_kwh": day_summary["total_import_kwh"],
                "export_kwh": day_summary["total_export_kwh"],
                "import_costs": day_summary["import_costs"],
                "export_earnings": day_summary["export_earnings"],
                "net": day_summary["net"],
            })
        return {"days": summaries}

    def import_ha_history(self, date: str) -> dict[str, Any]:
        """Fetch HA history for *date* (YYYY-MM-DD, local time) and backfill the
        price_tracking table.  Clears existing rows for that date first so the
        call is idempotent."""
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"date must be YYYY-MM-DD, got {date!r}")

        day_start = datetime.strptime(date, "%Y-%m-%d").replace(
            hour=0, minute=0, second=0, microsecond=0,
            tzinfo=self.tz,
        )
        now_local = datetime.now(self.tz)
        day_end = min(
            day_start.replace(hour=23, minute=59, second=59),
            now_local,
        )
        if day_end <= day_start:
            return {"date": date, "rows_inserted": 0, "rows_cleared": 0}

        e = self.cfg.entities
        entities = {
            "import": e.grid_import_power_sensor,
            "export": e.grid_export_power_sensor,
            "price": e.price_sensor,
            "feedin": e.feedin_sensor,
            "soc": e.battery_soc_sensor,
        }

        def _fetch(entity_id: str) -> list[tuple[datetime, float]]:
            if not entity_id:
                return []
            try:
                rows = self.client.get_history(
                    entity_id=entity_id,
                    start=day_start.astimezone(timezone.utc),
                    end=day_end.astimezone(timezone.utc),
                )
            except Exception as exc:
                LOG.warning("import_ha_history: failed to fetch %s: %s", entity_id, exc)
                return []
            out: list[tuple[datetime, float]] = []
            for row in rows:
                try:
                    v = float(row["state"])
                except (TypeError, ValueError):
                    continue
                ts_str = row.get("last_changed") or row.get("last_updated", "")
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(self.tz)
                except ValueError:
                    continue
                out.append((ts, v))
            return sorted(out, key=lambda x: x[0])

        series = {k: _fetch(v) for k, v in entities.items()}
        LOG.info(
            "import_ha_history %s: fetched %s",
            date,
            {k: len(v) for k, v in series.items()},
        )

        def _value_at(data: list[tuple[datetime, float]], ts: datetime) -> float | None:
            val = None
            for t, v in data:
                if t <= ts:
                    val = v
                else:
                    break
            return val

        def _to_kw(v: float | None) -> float:
            if v is None:
                return 0.0
            return max(0.0, v / 1000.0 if v > 100 else v)

        rows_to_insert: list[tuple] = []
        last_block: int | None = None
        last_imp = -999.0
        last_exp = -999.0

        current = day_start
        step = timedelta(seconds=20)
        while current <= day_end:
            block_num = int(current.timestamp()) // 300
            block_start_dt = datetime.fromtimestamp(block_num * 300, tz=self.tz)

            imp_kw = _to_kw(_value_at(series["import"], current))
            exp_kw = _to_kw(_value_at(series["export"], current))
            imp_price = _value_at(series["price"], current)
            feedin = _value_at(series["feedin"], current)
            soc = _value_at(series["soc"], current)

            new_block = last_block != block_num
            power_changed = abs(imp_kw - last_imp) > 0.25 or abs(exp_kw - last_exp) > 0.25

            if new_block or power_changed:
                rows_to_insert.append((
                    current.isoformat(timespec="seconds"),
                    block_start_dt.isoformat(timespec="seconds"),
                    round(imp_kw, 3),
                    round(exp_kw, 3),
                    imp_price,
                    feedin,
                    soc,
                ))
                last_block = block_num
                last_imp = imp_kw
                last_exp = exp_kw

            current += step

        rows_cleared = self.state_store.purge_day(date)
        for row in rows_to_insert:
            self.state_store.record_price_event(*row)

        LOG.info(
            "import_ha_history %s: cleared %d rows, inserted %d rows",
            date, rows_cleared, len(rows_to_insert),
        )
        return {
            "date": date,
            "rows_cleared": rows_cleared,
            "rows_inserted": len(rows_to_insert),
        }
