# Part of Optimizer — see optimizer/controller/__init__.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from optimizer.fit_export_window_analysis import WindowCandidates, get_export_window_candidates
from optimizer.ha_client import EntityState

LOG = logging.getLogger(__name__)


class _WindowsMixin:
    def _window_candidates(
        self,
        states: dict[str, EntityState],
        *,
        now: datetime,
        period: str,
        top_n: int = 10,
        morning_end_hour: int = 9,
        evening_start_hour: int = 19,
        daytime_start_hour: int = 9,
        daytime_end_hour: int = 15,
        window_minutes: int = 60,
        exclude_spike: bool = False,
    ):
        price_table = self._build_price_table(states, now=now)
        if not price_table:
            return WindowCandidates()
        try:
            return get_export_window_candidates(
                price_table,
                period=period,
                top_n=top_n,
                morning_end_hour=morning_end_hour,
                evening_start_hour=evening_start_hour,
                daytime_start_hour=daytime_start_hour,
                daytime_end_hour=daytime_end_hour,
                window_minutes=window_minutes,
                exclude_spike=exclude_spike,
            )
        except Exception:
            LOG.exception("FIT export window analysis failed for %s", period)
            return WindowCandidates()

    def _morning_dump_window(
        self,
        states: dict[str, EntityState],
        *,
        now: datetime,
        next_rising: datetime | None,
        preferred_hours: float,
        target_duration_h: float,
        productive_solar_dt: datetime | None = None,
    ) -> tuple[datetime | None, datetime | None]:
        if not next_rising or target_duration_h <= 0:
            return None, None
        # Anchor the end of the dump window on productive solar (PV ≥ load), not
        # bare sunrise.  PV typically takes 1-2 h after sunrise to exceed house
        # load, so ending at sunrise leaves the battery much too full.
        window_end = productive_solar_dt if productive_solar_dt is not None else next_rising
        min_duration_h = 0.5
        duration_h = max(min_duration_h, target_duration_h)
        default_start = window_end - timedelta(hours=duration_h)

        # Keep search reasonable and close to sunrise.
        max_search_back_h = min(4.0, max(preferred_hours * 2.0, preferred_hours + 0.5, duration_h + 0.5))
        earliest_start = window_end - timedelta(hours=max_search_back_h)
        latest_start = window_end - timedelta(hours=min_duration_h)
        if earliest_start >= latest_start:
            return default_start, window_end

        analysis_morning_end_hour = max(
            1,
            min(
                23,
                window_end.hour + (1 if (window_end.minute or window_end.second or window_end.microsecond) else 0),
            ),
        )
        candidates = self._window_candidates(
            states,
            now=now,
            period="morning",
            top_n=20,
            morning_end_hour=analysis_morning_end_hour,
            window_minutes=60,
            exclude_spike=True,
        )
        if not candidates:
            return default_start, window_end

        for _, row in candidates.iterrows():
            start_dt = self._to_local_aware(row.get("start_time"))
            if start_dt is None:
                continue
            if earliest_start <= start_dt <= latest_start:
                return start_dt, window_end
        return default_start, window_end

    def _hours_until_pv_exceeds_load(
        self,
        states: dict[str, EntityState],
        *,
        now: datetime,
        load_kw: float,
        fallback_hours: float,
    ) -> float:
        points = self._forecast_pv_points_kw(states, self.cfg.entities.forecast_today_sensor)
        if not points:
            return max(0.0, fallback_hours)
        threshold_kw = max(0.0, load_kw)
        for ts, pv_kw in points:
            if ts < now:
                continue
            if pv_kw >= threshold_kw:
                return max(0.0, (ts - now).total_seconds() / 3600.0)
        return max(0.0, fallback_hours)

    def compute_fit_windows_for_today(
        self,
        states: dict[str, EntityState],
        *,
        now: datetime,
        top_n: int = 5,
    ) -> dict[str, Any]:
        """Compute the top FIT export window candidates for morning, daytime and evening periods.

        Returns a dict with keys 'date', 'computed_at', 'morning', 'daytime', 'evening'.
        Each period value is a list of window dicts with 'start', 'end', 'avg_fit',
        'fit_at_start' (all ISO strings / floats).
        """
        t = self.cfg.thresholds
        late_morning_hour = max(1, min(23, getattr(t, "late_morning_hour", 11)))

        def _to_iso(dt: Any) -> str | None:
            resolved = self._to_local_aware(dt)
            return resolved.isoformat() if resolved else None

        def _format(candidates) -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            for _, row in candidates.iterrows():
                start = _to_iso(row.get("start_time"))
                end = _to_iso(row.get("window_end_time"))
                if start and end:
                    out.append({
                        "start": start,
                        "end": end,
                        "avg_fit": row.get("avg_fit_next_hour"),
                        "fit_at_start": row.get("fit_at_start"),
                    })
            return out

        morning = self._window_candidates(
            states,
            now=now,
            period="morning",
            top_n=top_n,
            morning_end_hour=late_morning_hour,
            window_minutes=60,
            exclude_spike=True,
        )
        evening = self._window_candidates(
            states,
            now=now,
            period="evening",
            top_n=top_n,
            evening_start_hour=19,
            window_minutes=60,
            exclude_spike=True,
        )
        return {
            "date": now.date().isoformat(),
            "computed_at": now.isoformat(),
            "morning": _format(morning),
            "daytime": [],
            "evening": _format(evening),
        }
