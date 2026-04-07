# Part of OptimizerRuntime — see optimizer/runtime/__init__.py
from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta
from typing import Any

LOG = logging.getLogger(__name__)


class _PersistenceMixin:
    def _restore_fit_windows_on_startup(self) -> None:
        """Restore today's FIT export windows from persistent storage after a restart."""
        try:
            today = datetime.now(self.tz).date().isoformat()
            doc = self.state_store.get_json(f"fit_windows_{today}") or {}
            if doc.get("date") == today:
                with self._lock:
                    self._fit_windows_cache = doc
                LOG.info("Restored FIT export windows for %s", today)
        except Exception:
            LOG.debug("Failed restoring FIT windows", exc_info=True)

    def _backfill_missing_earnings_on_startup(self) -> None:
        """Auto-import HA history for any of the last 7 days that have no
        price_tracking rows in the DB.  Runs in a daemon thread so it doesn't
        block startup or the eager first cycle."""
        def _run() -> None:
            now = datetime.now(self.tz)
            for i in range(6, -1, -1):
                date = (now - timedelta(days=i)).strftime("%Y-%m-%d")
                try:
                    if not self.state_store.get_price_events(date=date, limit=1):
                        LOG.info("Auto-backfilling earnings for %s from HA history", date)
                        self.import_ha_history(date)
                except Exception:
                    LOG.debug("Auto-backfill failed for %s", date, exc_info=True)
        t = threading.Thread(target=_run, daemon=True, name="earnings-backfill")
        t.start()

    def _compute_and_store_fit_windows(self) -> None:
        """Compute FIT export window candidates for today and persist them.

        Period data is only replaced when the fresh analysis produces results.
        Once we're past morning hours the morning analysis returns an empty list
        (no price rows before ``morning_end_hour`` remain in the forecast); in that
        case we keep the morning windows that were computed pre-sunrise and are still
        in the cache or persistent storage, so they continue to appear on the chart.
        """
        try:
            now = datetime.now(self.tz)
            states = self.client.get_all_states(use_cache=True)
            windows = self.optimizer.compute_fit_windows_for_today(states, now=now, top_n=5)
            date_str = now.date().isoformat()
            # Preserve any period whose analysis returned results previously but is
            # now empty (e.g. morning hours are past so no price rows qualify).
            with self._lock:
                existing = self._fit_windows_cache or {}
            if existing.get("date") == date_str:
                for period in ("morning", "daytime", "evening"):
                    if not windows.get(period) and existing.get(period):
                        windows[period] = existing[period]
            self.state_store.set_json(f"fit_windows_{date_str}", windows)
            with self._lock:
                self._fit_windows_cache = windows
            LOG.debug("Stored FIT export windows for %s", date_str)
        except Exception:
            LOG.debug("Failed computing/storing FIT windows", exc_info=True)

    def fit_windows_snapshot(self, date: str | None = None) -> dict[str, Any]:
        """Return FIT export window candidates for *date* (YYYY-MM-DD) or today."""
        if date:
            return self.state_store.get_json(f"fit_windows_{date}") or {}
        with self._lock:
            data = self._fit_windows_cache
        if not data:
            today = datetime.now(self.tz).date().isoformat()
            data = self.state_store.get_json(f"fit_windows_{today}") or {}
        return data or {}
