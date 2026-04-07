from __future__ import annotations

import logging
import os
import threading
from collections import deque
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from optimizer.config import AppConfig
from optimizer.controller import Decision, Optimizer
from optimizer.ha_client import EntityState, HAClient
from optimizer.state_store import StateStore
from optimizer.runtime._constants import CONTROL_MODES, ALGORITHM_TUNINGS
from optimizer.runtime._lifecycle import _LifecycleMixin
from optimizer.runtime._ha_bridge import _HaBridgeMixin
from optimizer.runtime._controls import _ControlsMixin
from optimizer.runtime._thresholds import _ThresholdsMixin
from optimizer.runtime._persistence import _PersistenceMixin
from optimizer.runtime._snapshots import _SnapshotsMixin
from optimizer.runtime._price_data import _PriceDataMixin
from optimizer.runtime._power_data import _PowerDataMixin
from optimizer.runtime._earnings import _EarningsMixin
from optimizer.runtime._simulation import _SimulationMixin

LOG = logging.getLogger(__name__)


class MemoryLogHandler(logging.Handler):
    def __init__(self, limit: int = 300) -> None:
        super().__init__()
        self._buffer: deque[dict[str, str]] = deque(maxlen=limit)
        self._lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        item = {
            "ts": datetime.fromtimestamp(record.created).isoformat(timespec="seconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        with self._lock:
            self._buffer.append(item)

    def get_logs(self) -> list[dict[str, str]]:
        with self._lock:
            return list(self._buffer)


class OptimizerRuntime(
    _LifecycleMixin, _HaBridgeMixin, _ControlsMixin, _ThresholdsMixin,
    _PersistenceMixin, _SnapshotsMixin, _PriceDataMixin, _PowerDataMixin,
    _EarningsMixin, _SimulationMixin,
):
    def __init__(self, config_path: str, timezone: str) -> None:
        self.config_path = config_path
        self.timezone = timezone
        self.cfg = AppConfig.load(config_path)
        self.cfg.validate()

        self.client = HAClient(self.cfg.ha_url, self.cfg.ha_token)
        self.optimizer = Optimizer(self.cfg, self.client, timezone=timezone)
        default_db_path = str(Path(config_path).resolve().parent / "optimizer_state.db")
        self.state_store = StateStore(os.environ.get("STATE_DB_PATH", default_db_path))

        self.poll_seconds = max(5, int(self.cfg.service.poll_seconds))
        self.tz = ZoneInfo(timezone)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

        self.control_mode: str = "automated"
        self.algorithm_tuning: str = "balanced"
        self._config_midnight_reserve_floor = float(self.cfg.thresholds.midnight_reserve_soc)
        self._base_thresholds: dict[str, Any] = deepcopy(self.cfg.thresholds.__dict__)

        self.last_cycle_started: str | None = None
        self.last_cycle_completed: str | None = None
        self.last_reload: str | None = None
        self.last_error: str | None = None
        self.last_decision: dict[str, Any] | None = None

        self._last_reload_dt: datetime | None = None
        self._price_history_cache: dict[str, dict[str, Any]] = {}
        self._restore_attempted = False
        self._last_autotune_day: str | None = None
        self._autotune_summary: dict[str, Any] | None = None
        self._sim_cache_block: int | None = None
        self._sim_cache_result: dict[str, Any] | None = None
        self._last_soc_int: int | None = None
        self._cycle_listeners: list[Callable[[], None]] = []
        self._fit_windows_cache: dict[str, Any] | None = None
        # Price tracking: record on new 5-min block, significant grid power change, or price change
        self._last_tracked_block: int | None = None
        self._last_tracked_import_kw: float = -999.0
        self._last_tracked_export_kw: float = -999.0
        self._last_tracked_import_price: float | None = None
        self._last_tracked_feedin_price: float | None = None
        self._refresh_effective_thresholds()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        def _deferred_start() -> None:
            # Brief pause so uvicorn finishes its startup sequence and the web
            # app is accepting requests before we run any heavy work.
            import time
            time.sleep(2)

            if not self._restore_attempted:
                self._restore_attempted = True
                self._restore_last_state_on_startup()
                self._restore_daily_tuning_for_today()
                self._restore_algorithm_tuning_on_startup()
                self._restore_fit_windows_on_startup()
                self._backfill_missing_earnings_on_startup()

            # Run one cycle eagerly so the UI is populated and hardware settings
            # are applied before the background thread's first poll interval.
            if self.get_control_mode() == "automated":
                try:
                    self._run_once()
                except Exception:
                    LOG.exception("Startup eager cycle failed — continuing to background thread")

            self._thread = threading.Thread(target=self._run, daemon=True, name="optimizer-worker")
            self._thread.start()
            LOG.info("Runtime started (poll=%ss)", self.poll_seconds)

        threading.Thread(target=_deferred_start, daemon=True, name="optimizer-startup").start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        # Stop WebSocket connection
        self.client.stop()

    def _now(self) -> str:
        return datetime.now().isoformat(timespec="seconds")


__all__ = ["MemoryLogHandler", "OptimizerRuntime", "CONTROL_MODES", "ALGORITHM_TUNINGS"]
