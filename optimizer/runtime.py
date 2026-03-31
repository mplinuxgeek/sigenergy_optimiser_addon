from __future__ import annotations

import logging
import os
import threading
import time
from bisect import bisect_right
from copy import deepcopy
from collections import deque
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from optimizer.config import AppConfig
from optimizer.controller import Decision, Optimizer
from optimizer.ha_client import EntityState, HAClient
from optimizer.state_store import StateStore

LOG = logging.getLogger(__name__)

CONTROL_MODES = {
    "automated",
    "manual",
    "force_full_export",
    "force_full_import",
    "prevent_import_export",
}

ALGORITHM_TUNINGS = {
    "balanced",
    "max_consumption",
    "max_profits",
}


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


class OptimizerRuntime:
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
        # Price tracking: record on new 5-min block, significant grid power change, or price change
        self._last_tracked_block: int | None = None
        self._last_tracked_import_kw: float = -999.0
        self._last_tracked_export_kw: float = -999.0
        self._last_tracked_import_price: float | None = None
        self._last_tracked_feedin_price: float | None = None
        self._refresh_effective_thresholds()

    def add_cycle_listener(self, callback: Callable[[], None]) -> None:
        """Register a callback to be invoked (from the worker thread) after each cycle."""
        self._cycle_listeners.append(callback)

    def remove_cycle_listener(self, callback: Callable[[], None]) -> None:
        try:
            self._cycle_listeners.remove(callback)
        except ValueError:
            pass

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        if not self._restore_attempted:
            self._restore_attempted = True
            self._restore_last_state_on_startup()
            self._restore_daily_tuning_for_today()
            self._restore_algorithm_tuning_on_startup()
        self._thread = threading.Thread(target=self._run, daemon=True, name="optimizer-worker")
        self._thread.start()
        LOG.info("Runtime started (poll=%ss)", self.poll_seconds)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        # Stop WebSocket connection
        self.client.stop()

    def _now(self) -> str:
        return datetime.now().isoformat(timespec="seconds")

    def _verify_and_reapply_if_needed(self, decision_dict: dict[str, Any]) -> None:
        """After _apply, re-read key entity states and retry up to 6 times if they
        haven't reached target values AND the pricing zone makes the mismatch costly
        (export revenue at stake, or negative-price import opportunity missed).
        Retries are skipped entirely when the system is in a neutral/zero-cost position.
        After 3 failed attempts the Sigenergy integration is reloaded (if
        sigenergy_config_entry_id is configured) to clear any stuck state before the
        remaining retries continue.
        """
        e = self.cfg.entities
        t = self.cfg.thresholds
        feedin_price = float(decision_dict.get("feedin_price", 0.0))
        current_price = float(decision_dict.get("current_price", 0.0))
        desired_export = float(decision_dict.get("desired_export_limit", 0.0))
        desired_import = float(decision_dict.get("desired_import_limit", 0.0))
        desired_mode = str(decision_dict.get("desired_mode", ""))

        export_revenue_at_stake = desired_export > 0 and feedin_price >= t.export_threshold_low
        import_savings_at_stake = desired_import > 0 and current_price < 0
        if not (export_revenue_at_stake or import_savings_at_stake):
            return

        off_band = t.off_setpoint_kw + 0.15
        # Must be >= min_change_threshold: _apply() skips writes smaller than
        # min_change_threshold, so we must also accept read-backs within that
        # same tolerance.  Halving it caused a deadlock where _is_settled()
        # rejected a 0.11 kW gap that _apply() would never rewrite.
        change_tol = max(0.15, t.min_change_threshold)

        def _read_float(states: dict[str, Any], entity_id: str) -> float:
            item = states.get(entity_id)
            if not item:
                return float("nan")
            try:
                return float(item.state)
            except (TypeError, ValueError):
                return float("nan")

        def _is_settled(states: dict[str, Any]) -> bool:
            mode_item = states.get(e.ems_mode_select)
            if desired_mode and (not mode_item or mode_item.state != desired_mode):
                return False
            cur_exp = _read_float(states, e.grid_export_limit)
            if cur_exp == cur_exp:  # not NaN
                if desired_export == 0:
                    if cur_exp > off_band:
                        return False
                elif abs(cur_exp - desired_export) > change_tol:
                    return False
            cur_imp = _read_float(states, e.grid_import_limit)
            if cur_imp == cur_imp:  # not NaN
                if desired_import == 0:
                    if cur_imp > off_band:
                        return False
                elif abs(cur_imp - desired_import) > change_tol:
                    return False
            return True

        d = Decision(**decision_dict)

        max_retries = 6
        reload_attempt = 3  # reload the integration after this many consecutive failures
        integration_reloaded = False

        for attempt in range(max_retries):
            # Trigger fresh inverter polls BEFORE sleeping so the HA entity cache
            # is warm by the time we read back.  The Sigenergy integration requires
            # an explicit update_entity to flush stale number/select states.
            for _eid in [e.ems_mode_select, e.grid_export_limit, e.grid_import_limit]:
                self._safe_action(
                    f"homeassistant.update_entity {_eid}",
                    lambda eid=_eid: self.client.update_entity(eid),
                )
            time.sleep(0.6 + attempt * 0.6)  # 0.6 s, 1.2 s, … 3.6 s
            states = self.client.get_all_states(use_cache=False)
            if _is_settled(states):
                if attempt > 0:
                    LOG.info(
                        "Post-apply settled after %d re-attempt(s): mode=%s export=%.2f import=%.2f",
                        attempt, desired_mode, desired_export, desired_import,
                    )
                return
            mode_item = states.get(e.ems_mode_select)
            cur_mode = mode_item.state if mode_item else "?"
            cur_exp = _read_float(states, e.grid_export_limit)
            cur_imp = _read_float(states, e.grid_import_limit)
            LOG.warning(
                "Post-apply not settled (attempt %d/%d): "
                "mode=%r→%r export=%.2f→%.2f import=%.2f→%.2f "
                "(feedin=%.3f price=%.3f); re-applying",
                attempt + 1, max_retries,
                cur_mode, desired_mode,
                cur_exp, desired_export,
                cur_imp, desired_import,
                feedin_price, current_price,
            )

            # After reload_attempt consecutive failures, reload the Sigenergy
            # integration to clear any stuck inverter communication state.
            if attempt + 1 == reload_attempt and not integration_reloaded:
                entry_id = (self.cfg.service.sigenergy_config_entry_id or "").strip()
                if entry_id:
                    LOG.warning(
                        "Post-apply: reloading Sigenergy integration (entry_id=%s) after %d failed attempts",
                        entry_id, reload_attempt,
                    )
                    self._safe_action(
                        f"homeassistant.reload_config_entry {entry_id}",
                        lambda: self.client.reload_config_entry(entry_id),
                    )
                    integration_reloaded = True
                    # Give the integration time to fully restart before the next attempt.
                    time.sleep(5.0)
                else:
                    LOG.warning(
                        "Post-apply: %d failures but sigenergy_config_entry_id not configured — skipping reload",
                        reload_attempt,
                    )

            self.optimizer._apply(states, d)

        # Final read after last _apply
        for _eid in [e.ems_mode_select, e.grid_export_limit, e.grid_import_limit]:
            self._safe_action(
                f"homeassistant.update_entity {_eid}",
                lambda eid=_eid: self.client.update_entity(eid),
            )
        time.sleep(0.6)
        states = self.client.get_all_states(use_cache=False)
        if not _is_settled(states):
            mode_item = states.get(e.ems_mode_select)
            cur_mode = mode_item.state if mode_item else "?"
            cur_exp = _read_float(states, e.grid_export_limit)
            cur_imp = _read_float(states, e.grid_import_limit)
            LOG.error(
                "Post-apply values did not stick after %d retries: "
                "mode=%r→%r export=%.2f→%.2f import=%.2f→%.2f",
                max_retries, cur_mode, desired_mode,
                cur_exp, desired_export, cur_imp, desired_import,
            )

    def _run_once(self) -> None:
        with self._lock:
            self.last_cycle_started = self._now()

        # Simulation is expensive (history fetches + full-day step loop); limit
        # it to once per 5-minute block.  Within the same block, re-apply the
        # cached decision to hardware so settings are still enforced every poll.
        now_block = int(datetime.now(self.tz).timestamp()) // 300
        with self._lock:
            cached_block = self._sim_cache_block
            cached_result = self._sim_cache_result

        if cached_block == now_block and cached_result is not None:
            LOG.debug("_run_once: cache HIT block=%d — re-applying cached decision", now_block)
            decision_dict = cached_result.get("applied_decision")
            if decision_dict:
                live_states = self.client.get_all_states()
                # Always recompute with live states — _compute is cheap (no HA calls)
                # and guarantees all guards (incl. midnight_reserve hard-stop) use the
                # actual current SoC rather than whatever was cached 0–5 min ago.
                live_decision = self.optimizer._compute(live_states)
                decision_dict = asdict(live_decision)
                # While discharging, bust the sim cache if:
                # - SoC dropped by 5+ percentage points (re-anchor the timeline), or
                # - SoC is within 10% of the export floor (tighten control near the limit).
                # Busting every integer boundary (1%) caused up to 10 extra simulations
                # per 30-min export session without meaningfully improving decisions.
                cached_mode = str(live_decision.desired_mode)
                if "Discharging" in cached_mode:
                    e = self.cfg.entities
                    soc_state = live_states.get(e.battery_soc_sensor)
                    try:
                        current_soc_int = int(float(soc_state.state)) if soc_state else None
                    except (TypeError, ValueError):
                        current_soc_int = None
                    with self._lock:
                        prev_soc_int = self._last_soc_int
                    if current_soc_int is not None and prev_soc_int is not None:
                        soc_change = abs(current_soc_int - prev_soc_int)
                        export_floor = int(live_decision.export_floor_soc)
                        approaching_floor = current_soc_int <= (export_floor + 10)
                        if soc_change >= 5 or approaching_floor:
                            LOG.info(
                                "SoC %d%% → %d%% (Δ%d%%, floor=%d%%, approaching=%s) — busting simulation cache",
                                prev_soc_int, current_soc_int, soc_change, export_floor, approaching_floor,
                            )
                            with self._lock:
                                self._sim_cache_block = None
                            cached_block = None  # next iteration will run a fresh simulation
                    if current_soc_int is not None:
                        with self._lock:
                            self._last_soc_int = current_soc_int
                self.optimizer._apply(live_states, live_decision)
        if cached_block != now_block or cached_result is None:  # replaces the else:
            LOG.debug("_run_once: cache MISS block=%d (prev=%s) — running fresh simulation", now_block, cached_block)
            result = self.simulate_automated(log_summary=False, context="live", apply_now=True)
            with self._lock:
                self._sim_cache_block = now_block
                self._sim_cache_result = result
            decision_dict = result.get("applied_decision")
            # Refresh the baseline SoC so next-boundary detection starts clean.
            e = self.cfg.entities
            try:
                _soc_st = self.client.get_all_states().get(e.battery_soc_sensor)
                _soc_int = int(float(_soc_st.state)) if _soc_st else None
            except (TypeError, ValueError):
                _soc_int = None
            if _soc_int is not None:
                with self._lock:
                    self._last_soc_int = _soc_int

        if decision_dict:
            self._verify_and_reapply_if_needed(decision_dict)

        self._record_price_tracking(now_block)

        with self._lock:
            self.last_decision = decision_dict
            self.last_cycle_completed = self._now()
            self.last_error = None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._maybe_run_daily_autotune()
                mode = self.get_control_mode()
                if mode == "automated":
                    self._run_once()
                else:
                    with self._lock:
                        self.last_cycle_completed = self._now()
                        self.last_error = None
            except Exception as exc:
                LOG.exception("Optimizer cycle failed")
                with self._lock:
                    self.last_error = str(exc)
                    self.last_cycle_completed = self._now()
            for _cb in list(self._cycle_listeners):
                try:
                    _cb()
                except Exception:
                    pass
            self._stop.wait(self.poll_seconds)

    def force_cycle(self, *, source: str = "manual") -> dict[str, Any]:
        LOG.info("Action trigger (%s): force_cycle requested", source)
        if self.get_control_mode() != "automated":
            LOG.info("Action trigger (%s): force_cycle skipped (control_mode=%s)", source, self.get_control_mode())
            return self.status()
        with self._lock:
            self._sim_cache_block = None  # Bypass block cache for an immediate fresh simulation
        self._run_once()
        return self.status()

    def get_control_mode(self) -> str:
        with self._lock:
            return self.control_mode

    def set_control_mode(self, mode: str, *, source: str = "runtime") -> dict[str, Any]:
        mode = (mode or "").strip().lower()
        if mode not in CONTROL_MODES:
            raise ValueError(f"Unsupported control mode: {mode}")

        with self._lock:
            previous_mode = self.control_mode
            self.control_mode = mode
            if mode == "automated":
                # Stale cached simulation from before the mode change would apply
                # outdated decisions on the first cycle back; force a fresh run.
                self._sim_cache_block = None

        LOG.info("Action trigger (%s): set_control_mode %s -> %s", source, previous_mode, mode)

        e = self.cfg.entities

        # Blueprint parity: automated mode enables optimiser automation,
        # any non-automated mode disables it and stops running actions.
        automation_entity = e.optimiser_automation.strip()
        if automation_entity:
            if mode == "automated":
                self._safe_action(
                    f"automation.turn_on {automation_entity}",
                    lambda: self.client.automation_on(automation_entity),
                )
            else:
                self._safe_action(
                    f"automation.turn_off {automation_entity} (stop_actions=True)",
                    lambda: self.client.automation_off(automation_entity, stop_actions=True),
                )

        # Optional bridge: if user configured an HA input_select mode helper,
        # keep it in sync with the web control mode.
        mode_select_entity = e.manual_mode_select.strip()
        if mode_select_entity:
            label_map = {
                "automated": self.cfg.thresholds.automated_option,
                "manual": self.cfg.thresholds.manual_option,
                "force_full_export": self.cfg.thresholds.full_export_option,
                "force_full_import": self.cfg.thresholds.full_import_option,
                "prevent_import_export": self.cfg.thresholds.block_flow_option,
            }
            label = label_map.get(mode)
            if label:
                self._safe_action(
                    f"input_select.select_option {mode_select_entity}={label}",
                    lambda: self.client.set_input_select(mode_select_entity, label),
                )

        try:
            if mode == "force_full_export":
                self._apply_force_full_export()
            elif mode == "force_full_import":
                self._apply_force_full_import()
            elif mode == "prevent_import_export":
                self._apply_prevent_import_export()
            elif mode == "manual":
                self._apply_manual_mode()
        except Exception:
            with self._lock:
                self.control_mode = previous_mode
            raise

        self._persist_control_mode(mode)
        return self.status()

    def _persist_control_mode(self, mode: str) -> None:
        try:
            self.state_store.set_json("control_mode", {"mode": mode, "saved_at": self._now()})
        except Exception as exc:
            LOG.warning("Failed persisting control mode: %s", exc)

    def _persist_ess_settings(
        self,
        *,
        ems_mode: str | None,
        ha_control: bool | None,
        export_limit: float | None,
        import_limit: float | None,
        pv_max_power_limit: float | None,
    ) -> None:
        try:
            payload: dict[str, Any] = {"saved_at": self._now()}
            if ems_mode is not None:
                payload["ems_mode"] = str(ems_mode)
            if ha_control is not None:
                payload["ha_control"] = bool(ha_control)
            if export_limit is not None:
                payload["export_limit"] = float(export_limit)
            if import_limit is not None:
                payload["import_limit"] = float(import_limit)
            if pv_max_power_limit is not None:
                payload["pv_max_power_limit"] = float(pv_max_power_limit)
            self.state_store.set_json("ess_settings", payload)
        except Exception as exc:
            LOG.warning("Failed persisting ESS settings: %s", exc)

    def _restore_last_state_on_startup(self) -> None:
        try:
            mode_doc = self.state_store.get_json("control_mode") or {}
            ess_doc = self.state_store.get_json("ess_settings") or {}
            stored_mode = str(mode_doc.get("mode", "")).strip().lower()
            if stored_mode and stored_mode not in CONTROL_MODES:
                stored_mode = ""

            has_ess = any(
                k in ess_doc for k in ("ems_mode", "ha_control", "export_limit", "import_limit", "pv_max_power_limit")
            )

            if stored_mode == "manual":
                if has_ess:
                    LOG.info("Restoring persisted manual ESS settings on startup")
                    self.apply_ess_controls(
                        ems_mode=ess_doc.get("ems_mode"),
                        ha_control=ess_doc.get("ha_control"),
                        export_limit=ess_doc.get("export_limit"),
                        import_limit=ess_doc.get("import_limit"),
                        pv_max_power_limit=ess_doc.get("pv_max_power_limit"),
                        source="startup_restore",
                    )
                else:
                    LOG.info("Restoring persisted control mode on startup: %s", stored_mode)
                    self.set_control_mode("manual", source="startup_restore")
                return

            if stored_mode:
                LOG.info("Restoring persisted control mode on startup: %s", stored_mode)
                self.set_control_mode(stored_mode, source="startup_restore")
                return

            if has_ess:
                LOG.info("Restoring persisted ESS settings on startup")
                self.apply_ess_controls(
                    ems_mode=ess_doc.get("ems_mode"),
                    ha_control=ess_doc.get("ha_control"),
                    export_limit=ess_doc.get("export_limit"),
                    import_limit=ess_doc.get("import_limit"),
                    pv_max_power_limit=ess_doc.get("pv_max_power_limit"),
                    source="startup_restore",
                )
        except Exception:
            LOG.exception("Failed restoring persisted state on startup")

    def _safe_action(self, label: str, fn: Callable[[], None]) -> bool:
        try:
            fn()
            return True
        except Exception as exc:
            LOG.warning("%s failed: %s", label, exc)
            return False

    def _read_state_float(self, entity_id: str, default: float = 0.0) -> float:
        states = self.client.get_all_states()
        item = states.get(entity_id)
        if not item:
            return default
        try:
            return float(item.state)
        except (TypeError, ValueError):
            return default

    def _sensor_kw(self, entity_id: str, fallback: float) -> float:
        raw = self._read_state_float(entity_id, fallback)
        if raw <= 0:
            return fallback
        if raw > 1000:
            return raw / 1000.0
        return raw

    def _record_price_tracking(self, now_block: int) -> None:
        """Record a price-tracking event whenever a new 5-min billing block starts
        or when grid import/export power changes by more than 0.25 kW."""
        e = self.cfg.entities
        states = self.client.get_all_states()

        # Read grid power (sensors may report W or kW — _sensor_kw normalises to kW)
        import_raw = self._read_state_float(e.grid_import_power_sensor, 0.0)
        export_raw = self._read_state_float(e.grid_export_power_sensor, 0.0)
        import_kw = max(0.0, import_raw / 1000.0 if import_raw > 100 else import_raw)
        export_kw = max(0.0, export_raw / 1000.0 if export_raw > 100 else export_raw)

        with self._lock:
            last_block = self._last_tracked_block
            last_import = self._last_tracked_import_kw
            last_export = self._last_tracked_export_kw
            last_import_price = self._last_tracked_import_price
            last_feedin_price = self._last_tracked_feedin_price

        # Read prices before deciding whether to record
        try:
            import_price_item = states.get(e.price_sensor)
            import_price = float(import_price_item.state) if import_price_item else None
        except (TypeError, ValueError):
            import_price = None
        try:
            feedin_price_item = states.get(e.feedin_sensor)
            feedin_price = float(feedin_price_item.state) if feedin_price_item else None
        except (TypeError, ValueError):
            feedin_price = None

        new_block = last_block != now_block
        power_changed = abs(import_kw - last_import) > 0.25 or abs(export_kw - last_export) > 0.25
        price_changed = (
            (import_price is not None and last_import_price is not None and abs(import_price - last_import_price) > 0.001)
            or (feedin_price is not None and last_feedin_price is not None and abs(feedin_price - last_feedin_price) > 0.001)
            or (import_price is not None and last_import_price is None)
            or (feedin_price is not None and last_feedin_price is None)
        )

        if not new_block and not power_changed and not price_changed:
            return
        try:
            soc_item = states.get(e.battery_soc_sensor)
            battery_soc = float(soc_item.state) if soc_item else None
        except (TypeError, ValueError):
            battery_soc = None

        now_dt = datetime.now(self.tz)
        block_start = datetime.fromtimestamp(now_block * 300, tz=self.tz)
        ts = now_dt.isoformat(timespec="seconds")
        block_ts = block_start.isoformat(timespec="seconds")

        try:
            self.state_store.record_price_event(
                ts=ts,
                block_ts=block_ts,
                grid_import_kw=round(import_kw, 3),
                grid_export_kw=round(export_kw, 3),
                import_price=import_price,
                feedin_price=feedin_price,
                battery_soc=battery_soc,
            )
        except Exception:
            LOG.debug("Failed to record price tracking event", exc_info=True)

        with self._lock:
            self._last_tracked_block = now_block
            self._last_tracked_import_kw = import_kw
            self._last_tracked_export_kw = export_kw
            if import_price is not None:
                self._last_tracked_import_price = import_price
            if feedin_price is not None:
                self._last_tracked_feedin_price = feedin_price

    def _set_number_clamped(self, entity_id: str, value: float) -> float:
        states = self.client.get_all_states()
        item = states.get(entity_id)
        bounded = float(value)
        if item:
            attrs = item.attributes or {}
            try:
                min_v = float(attrs.get("min")) if attrs.get("min") is not None else None
            except (ValueError, TypeError):
                min_v = None
            try:
                max_v = float(attrs.get("max")) if attrs.get("max") is not None else None
            except (ValueError, TypeError):
                max_v = None
            if min_v is not None and bounded < min_v:
                bounded = min_v
            if max_v is not None and bounded > max_v:
                bounded = max_v
        self.client.set_number(entity_id, bounded)
        return bounded

    def _set_optional_number(self, entity_id: str, value: float) -> None:
        eid = (entity_id or "").strip()
        if not eid:
            return
        desired = float(value)
        states = self.client.get_all_states()
        item = states.get(eid)
        target = desired
        if item:
            attrs = item.attributes or {}
            try:
                min_v = float(attrs.get("min")) if attrs.get("min") is not None else None
            except (ValueError, TypeError):
                min_v = None
            try:
                max_v = float(attrs.get("max")) if attrs.get("max") is not None else None
            except (ValueError, TypeError):
                max_v = None
            if min_v is not None and target < min_v:
                target = min_v
            if max_v is not None and target > max_v:
                target = max_v

        for _ in range(4):
            ok = self._safe_action(
                f"number.set_value {eid}={desired}",
                lambda: self._set_number_clamped(eid, desired),
            )
            if ok:
                self._safe_action(
                    f"homeassistant.update_entity {eid}",
                    lambda: self.client.update_entity(eid),
                )
                time.sleep(0.6)
                current = self._read_state_float(eid, float("nan"))
                if not (current != current) and abs(current - target) < 0.011:
                    return
            time.sleep(0.25)

        current = self._read_state_float(eid, float("nan"))
        if current == current:
            LOG.warning(
                "number.set_value %s did not stick (wanted %s, target %s, actual %s)",
                eid,
                desired,
                target,
                current,
            )
        else:
            LOG.warning("number.set_value %s failed and current state is unavailable", eid)

    def _read_state_text(self, entity_id: str, default: str = "unknown") -> str:
        states = self.client.get_all_states()
        item = states.get(entity_id)
        if not item:
            return default
        return str(item.state)

    def _ensure_select_state(self, entity_id: str, option: str, retries: int = 5) -> bool:
        def _attempt_set(target: str, n: int) -> bool:
            if self._read_state_text(entity_id, "unknown") == target:
                return True
            for _ in range(n):
                ok = self._safe_action(
                    f"select.select_option {entity_id}={target}",
                    lambda: self.client.set_select(entity_id, target),
                )
                if ok:
                    self._safe_action(
                        f"homeassistant.update_entity {entity_id}",
                        lambda: self.client.update_entity(entity_id),
                    )
                    time.sleep(0.5)
                    if self._read_state_text(entity_id, "unknown") == target:
                        return True
                time.sleep(0.35)
            return False

        if _attempt_set(option, retries):
            return True

        # Some firmware/integration states appear to reject direct transitions to
        # Maximum Self Consumption. A hop through Standby improves reliability.
        if option.strip().lower() == "maximum self consumption":
            if _attempt_set("Standby", 2):
                return _attempt_set(option, retries)

        return False

    @staticmethod
    def _mode_is_command_charging(ems_mode: str) -> bool:
        return "command charging" in (ems_mode or "").strip().lower()

    @staticmethod
    def _mode_is_command_discharging(ems_mode: str) -> bool:
        return "command discharging" in (ems_mode or "").strip().lower()

    def _apply_ess_limits_for_mode(self, ems_mode: str) -> None:
        e = self.cfg.entities
        t = self.cfg.thresholds

        require_command_modes = bool(t.ess_limits_require_command_modes)
        allow_charge_limit = (not require_command_modes) or self._mode_is_command_charging(ems_mode)
        allow_discharge_limit = (not require_command_modes) or self._mode_is_command_discharging(ems_mode)

        if allow_charge_limit:
            self._set_optional_number(e.ess_max_charging_limit, t.ess_limit_value)
        elif (e.ess_max_charging_limit or "").strip():
            LOG.info(
                "Skipping ESS max charging limit in mode '%s' (legacy mode-aware limits enabled)",
                ems_mode,
            )

        if allow_discharge_limit:
            self._set_optional_number(e.ess_max_discharging_limit, t.ess_limit_value)
        elif (e.ess_max_discharging_limit or "").strip():
            LOG.info(
                "Skipping ESS max discharging limit in mode '%s' (legacy mode-aware limits enabled)",
                ems_mode,
            )

    def _apply_force_full_export(self) -> None:
        e = self.cfg.entities
        t = self.cfg.thresholds
        computed_export = (
            self._sensor_kw(e.ess_rated_discharge_power_sensor, t.export_limit_value)
            if bool(t.force_mode_use_rated_limits)
            else float(t.export_limit_value)
        )

        self._safe_action(
            f"switch.turn_on {e.ha_control_switch}",
            lambda: self.client.switch_on(e.ha_control_switch),
        )
        if not self._ensure_select_state(e.ems_mode_select, t.export_mode_option):
            LOG.warning("EMS mode did not reach '%s' while applying force_full_export", t.export_mode_option)
        self._safe_action(
            f"homeassistant.update_entity {e.ems_mode_select}",
            lambda: self.client.update_entity(e.ems_mode_select),
        )
        time.sleep(1.0)
        self._apply_ess_limits_for_mode(t.export_mode_option)
        self._set_optional_number(e.grid_export_limit, computed_export)
        self._set_optional_number(e.grid_import_limit, t.off_setpoint_kw)
        self._set_optional_number(e.pv_max_power_limit, t.pv_max_power_value)
        self._safe_action(
            f"input_boolean.turn_on {e.automated_export_flag}",
            lambda: self.client.bool_on(e.automated_export_flag),
        )
        LOG.info("Applied force_full_export (computed_export=%s)", round(computed_export, 3))

    def _apply_force_full_import(self) -> None:
        e = self.cfg.entities
        t = self.cfg.thresholds
        computed_import = (
            self._sensor_kw(e.ess_rated_charge_power_sensor, t.import_limit_value)
            if bool(t.force_mode_use_rated_limits)
            else float(t.import_limit_value)
        )

        self._safe_action(
            f"switch.turn_on {e.ha_control_switch}",
            lambda: self.client.switch_on(e.ha_control_switch),
        )
        if not self._ensure_select_state(e.ems_mode_select, t.import_mode_option):
            LOG.warning("EMS mode did not reach '%s' while applying force_full_import", t.import_mode_option)
        self._safe_action(
            f"homeassistant.update_entity {e.ems_mode_select}",
            lambda: self.client.update_entity(e.ems_mode_select),
        )
        time.sleep(1.0)
        self._apply_ess_limits_for_mode(t.import_mode_option)
        self._set_optional_number(e.grid_export_limit, t.off_setpoint_kw)
        self._set_optional_number(e.grid_import_limit, computed_import)
        self._set_optional_number(e.pv_max_power_limit, t.pv_max_power_value)
        self._safe_action(
            f"input_boolean.turn_off {e.automated_export_flag}",
            lambda: self.client.bool_off(e.automated_export_flag),
        )
        LOG.info("Applied force_full_import (computed_import=%s)", round(computed_import, 3))

    def _apply_prevent_import_export(self) -> None:
        e = self.cfg.entities
        t = self.cfg.thresholds
        self._safe_action(
            f"switch.turn_on {e.ha_control_switch}",
            lambda: self.client.switch_on(e.ha_control_switch),
        )
        if not self._ensure_select_state(e.ems_mode_select, t.block_mode_option):
            LOG.warning("EMS mode did not reach '%s' while applying prevent_import_export", t.block_mode_option)
        self._safe_action(
            f"homeassistant.update_entity {e.ems_mode_select}",
            lambda: self.client.update_entity(e.ems_mode_select),
        )
        time.sleep(1.0)
        self._apply_ess_limits_for_mode(t.block_mode_option)
        self._set_optional_number(e.grid_export_limit, t.off_setpoint_kw)
        self._set_optional_number(e.grid_import_limit, t.off_setpoint_kw)
        self._set_optional_number(e.pv_max_power_limit, t.pv_max_power_value)
        self._safe_action(
            f"input_boolean.turn_off {e.automated_export_flag}",
            lambda: self.client.bool_off(e.automated_export_flag),
        )
        LOG.info("Applied prevent_import_export")

    def _apply_manual_mode(self) -> None:
        e = self.cfg.entities
        manual_ems_mode = "Command Charging (PV First)"
        self._safe_action(
            f"switch.turn_on {e.ha_control_switch}",
            lambda: self.client.switch_on(e.ha_control_switch),
        )
        if not self._ensure_select_state(e.ems_mode_select, manual_ems_mode):
            LOG.warning("EMS mode did not reach '%s' while applying manual mode", manual_ems_mode)
        self._safe_action(
            f"homeassistant.update_entity {e.ems_mode_select}",
            lambda: self.client.update_entity(e.ems_mode_select),
        )
        time.sleep(1.0)
        self._set_optional_number(e.grid_export_limit, 0.0)
        self._set_optional_number(e.grid_import_limit, 0.0)
        self._set_optional_number(e.pv_max_power_limit, 25.0)
        self._safe_action(
            f"input_boolean.turn_off {e.automated_export_flag}",
            lambda: self.client.bool_off(e.automated_export_flag),
        )
        LOG.info("Applied manual mode")

    def _force_sync_entities(self, entity_ids: list[str]) -> None:
        ids: list[str] = []
        for eid in entity_ids:
            eid = (eid or "").strip()
            if not eid or eid in ids:
                continue
            ids.append(eid)
        if not ids:
            return
        self._safe_action(
            f"homeassistant.update_entity {', '.join(ids)}",
            lambda: self.client.update_entities(ids),
        )

    def apply_ess_controls(
        self,
        *,
        ems_mode: str | None,
        ha_control: bool | None,
        export_limit: float | None,
        import_limit: float | None,
        pv_max_power_limit: float | None,
        source: str = "runtime",
    ) -> dict[str, Any]:
        e = self.cfg.entities
        t = self.cfg.thresholds
        hard_failures: list[str] = []
        soft_failures: list[str] = []
        touched_entities: list[str] = []

        def _current_state(entity_id: str) -> str:
            item = self.client.get_all_states().get(entity_id)
            return item.state if item else "unknown"

        def _ensure_switch(entity_id: str, turn_on: bool) -> bool:
            desired = "on" if turn_on else "off"
            if _current_state(entity_id) == desired:
                return True
            for _ in range(2):
                ok = self._safe_action(
                    f"switch.turn_{'on' if turn_on else 'off'} {entity_id}",
                    (lambda: self.client.switch_on(entity_id)) if turn_on else (lambda: self.client.switch_off(entity_id)),
                )
                if ok:
                    time.sleep(0.35)
                    if _current_state(entity_id) == desired:
                        return True
                time.sleep(0.2)
            return False

        def _ensure_select(entity_id: str, option: str) -> bool:
            if _current_state(entity_id) == option:
                return True
            for _ in range(3):
                self._safe_action(
                    f"select.select_option {entity_id}={option}",
                    lambda: self.client.set_select(entity_id, option),
                )
                # Always call update_entity: after a 500 (which the Sigenergy
                # integration frequently returns even on successful writes) the
                # HA state cache is stale until a forced re-poll is triggered.
                self._safe_action(
                    f"homeassistant.update_entity {entity_id}",
                    lambda: self.client.update_entity(entity_id),
                )
                time.sleep(0.5)
                if _current_state(entity_id) == option:
                    return True
            return False

        def _ensure_number(entity_id: str, desired: float) -> tuple[bool, float | None, float]:
            desired_f = float(desired)
            target = desired_f
            item = self.client.get_all_states().get(entity_id)
            if item:
                attrs = item.attributes or {}
                try:
                    min_v = float(attrs.get("min")) if attrs.get("min") is not None else None
                except (ValueError, TypeError):
                    min_v = None
                try:
                    max_v = float(attrs.get("max")) if attrs.get("max") is not None else None
                except (ValueError, TypeError):
                    max_v = None
                if min_v is not None and target < min_v:
                    target = min_v
                if max_v is not None and target > max_v:
                    target = max_v

            for _ in range(4):
                ok = self._safe_action(
                    f"number.set_value {entity_id}={desired_f}",
                    lambda: self._set_number_clamped(entity_id, desired_f),
                )
                if ok:
                    self._safe_action(
                        f"homeassistant.update_entity {entity_id}",
                        lambda: self.client.update_entity(entity_id),
                    )
                    time.sleep(0.6)
                    current = self._read_state_float(entity_id, float('nan'))
                    if not (current != current) and abs(current - target) < 0.011:
                        return True, current, target
                time.sleep(0.25)
            current = self._read_state_float(entity_id, float('nan'))
            if current != current:
                return False, None, target
            return False, current, target

        LOG.info(
            "Action trigger (%s): apply_ess_controls requested (ha_control=%s, mode=%s, export=%s, import=%s, pv_max=%s)",
            source,
            ha_control,
            ems_mode,
            export_limit,
            import_limit,
            pv_max_power_limit,
        )

        with self._lock:
            was_automated = self.control_mode == "automated"
            if was_automated:
                self.control_mode = "manual"
                LOG.info("ESS controls requested; switching runtime control mode to manual")
                self._persist_control_mode("manual")

        if was_automated:
            # Mirror the side-effects of set_control_mode("manual"): turn off the
            # optimiser automation helper and sync the mode-select bridge entity so
            # HA dashboards stay consistent.
            e_cfg = self.cfg.entities
            automation_entity = e_cfg.optimiser_automation.strip()
            if automation_entity:
                self._safe_action(
                    f"automation.turn_off {automation_entity} (stop_actions=True)",
                    lambda: self.client.automation_off(automation_entity, stop_actions=True),
                )
            mode_select_entity = e_cfg.manual_mode_select.strip()
            if mode_select_entity:
                label = self.cfg.thresholds.manual_option
                if label:
                    self._safe_action(
                        f"input_select.select_option {mode_select_entity}={label}",
                        lambda: self.client.set_input_select(mode_select_entity, label),
                    )

        if ha_control is not None:
            touched_entities.append(e.ha_control_switch)
            if not _ensure_switch(e.ha_control_switch, ha_control):
                soft_failures.append(f"HA control switch did not reach {'on' if ha_control else 'off'}")

        if ems_mode:
            touched_entities.append(e.ems_mode_select)
            if not _ensure_select(e.ems_mode_select, ems_mode):
                # Sigenergy HA integrations can intermittently return 500 for select writes;
                # treat this as a soft failure so other control changes are still applied.
                soft_failures.append(f"EMS mode did not reach '{ems_mode}'")

        if export_limit is not None:
            touched_entities.append(e.grid_export_limit)
            ok, actual, target = _ensure_number(e.grid_export_limit, export_limit)
            if not ok:
                if actual is None:
                    soft_failures.append(f"Grid export limit write failed ({export_limit})")
                else:
                    soft_failures.append(
                        f"Grid export limit did not stick (wanted {export_limit}, target {target}, actual {actual})"
                    )

        effective_ems_mode = (ems_mode or _current_state(e.ems_mode_select) or "").strip()
        if import_limit is not None and effective_ems_mode and self._mode_is_command_discharging(effective_ems_mode):
            coerced = float(t.off_setpoint_kw)
            if abs(float(import_limit) - coerced) >= 0.011:
                soft_failures.append(
                    f"Import limit {import_limit} coerced to {coerced} in EMS mode '{effective_ems_mode}'"
                )
            import_limit = coerced

        if import_limit is not None:
            touched_entities.append(e.grid_import_limit)
            ok, actual, target = _ensure_number(e.grid_import_limit, import_limit)
            if not ok:
                if actual is None:
                    soft_failures.append(f"Grid import limit write failed ({import_limit})")
                else:
                    soft_failures.append(
                        f"Grid import limit did not stick (wanted {import_limit}, target {target}, actual {actual})"
                    )

        if pv_max_power_limit is not None:
            touched_entities.append(e.pv_max_power_limit)
            ok, actual, target = _ensure_number(e.pv_max_power_limit, pv_max_power_limit)
            if not ok:
                if actual is None:
                    soft_failures.append(f"PV max power limit write failed ({pv_max_power_limit})")
                else:
                    soft_failures.append(
                        f"PV max power limit did not stick (wanted {pv_max_power_limit}, target {target}, actual {actual})"
                    )

        if soft_failures:
            LOG.warning("ESS controls applied with warnings: %s", "; ".join(soft_failures))

        if hard_failures:
            raise RuntimeError('; '.join(hard_failures + soft_failures))

        # Trigger an immediate state refresh of touched entities to reduce stale values
        # without requiring full integration reloads.
        if touched_entities:
            self._force_sync_entities(touched_entities)

        LOG.info(
            "Applied ESS controls (ha_control=%s, mode=%s, export=%s, import=%s, pv_max=%s)",
            ha_control,
            ems_mode,
            export_limit,
            import_limit,
            pv_max_power_limit,
        )
        self._persist_ess_settings(
            ems_mode=ems_mode,
            ha_control=ha_control,
            export_limit=export_limit,
            import_limit=import_limit,
            pv_max_power_limit=pv_max_power_limit,
        )
        snapshot = self.controls_snapshot()
        if soft_failures:
            snapshot["warnings"] = soft_failures
        return snapshot

    def controls_snapshot(self) -> dict[str, Any]:
        e = self.cfg.entities
        states = self.client.get_all_states()

        def pick(eid: str) -> dict[str, Any]:
            item = states.get(eid)
            return {
                "entity_id": eid,
                "state": item.state if item else "unknown",
                "attributes": item.attributes if item else {},
            }

        mode_info = pick(e.ems_mode_select)
        return {
            "control_mode": self.get_control_mode(),
            "control_modes": sorted(CONTROL_MODES),
            "algorithm_tuning": self.algorithm_tuning,
            "algorithm_tuning_options": sorted(ALGORITHM_TUNINGS),
            "ess": {
                "ha_control_switch": pick(e.ha_control_switch),
                "ems_mode_select": mode_info,
                "ems_mode_options": (mode_info.get("attributes", {}).get("options") or []),
                "grid_export_limit": pick(e.grid_export_limit),
                "grid_import_limit": pick(e.grid_import_limit),
                "pv_max_power_limit": pick(e.pv_max_power_limit),
            },
        }

    def status(self) -> dict[str, Any]:
        t = self.cfg.thresholds
        with self._lock:
            return {
                "last_cycle_started": self.last_cycle_started,
                "last_cycle_completed": self.last_cycle_completed,
                "last_reload": self.last_reload,
                "last_error": self.last_error,
                "poll_seconds": self.poll_seconds,
                "decision": self.last_decision,
                "control_mode": self.control_mode,
                "algorithm_tuning": self.algorithm_tuning,
                "autotune": self._autotune_summary,
                "thresholds": {
                    "export_threshold_low": t.export_threshold_low,
                    "export_threshold_medium": t.export_threshold_medium,
                    "export_threshold_high": t.export_threshold_high,
                    "export_limit_low": t.export_limit_low,
                    "export_limit_medium": t.export_limit_medium,
                    "export_limit_high": t.export_limit_high,
                    "import_threshold_low": t.import_threshold_low,
                    "import_threshold_medium": t.import_threshold_medium,
                    "import_threshold_high": t.import_threshold_high,
                    "import_limit_low": t.import_limit_low,
                    "import_limit_medium": t.import_limit_medium,
                    "import_limit_high": t.import_limit_high,
                    "ess_first_discharge_pv_threshold_kw": t.ess_first_discharge_pv_threshold_kw,
                },
                "base_thresholds": deepcopy(self._base_thresholds),
            }

    def key_entities_snapshot(self) -> dict[str, Any]:
        e = self.cfg.entities
        states = self.client.get_all_states()
        ids = {
            "battery_soc": e.battery_soc_sensor,
            "pv_power": e.pv_power_sensor,
            "load_power": e.consumed_power_sensor,
            "price": e.price_sensor,
            "feedin": e.feedin_sensor,
            "mode": e.ems_mode_select,
            "grid_export_limit": e.grid_export_limit,
            "grid_import_limit": e.grid_import_limit,
            "pv_max_power_limit": e.pv_max_power_limit,
            "forecast_today": e.forecast_today_sensor,
            "forecast_tomorrow": e.forecast_tomorrow_sensor,
            "forecast_remaining": e.forecast_remaining_sensor,
        }
        out: dict[str, Any] = {}
        for key, entity_id in ids.items():
            item = states.get(entity_id)
            out[key] = {
                "entity_id": entity_id,
                "state": item.state if item else "unknown",
                "attributes": item.attributes if item else {},
            }
        return out

    def public_config(self) -> dict[str, Any]:
        return {
            "home_assistant": {
                "url": self.cfg.ha_url,
                "token": "***",
            },
            "service": self.cfg.service.__dict__,
            "entities": self.cfg.entities.__dict__,
            "thresholds": self.cfg.thresholds.__dict__,
            "base_thresholds": deepcopy(self._base_thresholds),
            "profile_overrides": deepcopy(self.cfg.profile_overrides),
            "algorithm_tuning": self.algorithm_tuning,
        }

    def _apply_threshold_params(self, params: dict[str, Any]) -> None:
        for key, value in params.items():
            if key in self._base_thresholds:
                # midnight_reserve_soc is a user-set safety floor; never let any
                # caller (autotune, API) lower it below the base config value.
                if key == "midnight_reserve_soc":
                    value = max(float(value), self._config_midnight_reserve_floor)
                self._base_thresholds[key] = value
        self._refresh_effective_thresholds()

    def update_thresholds(self, params: dict[str, Any], *, source: str = "api") -> dict[str, Any]:
        """Update one or more threshold values at runtime. Changes take effect on the
        next optimizer cycle. Does not persist to disk."""
        LOG.info("Action trigger (%s): update_thresholds %s", source, params)
        with self._lock:
            self._apply_threshold_params(params)
            self._sim_cache_block = None  # force fresh simulation with new thresholds
        return {
            "thresholds": deepcopy(self.cfg.thresholds.__dict__),
            "base_thresholds": deepcopy(self._base_thresholds),
            "algorithm_tuning": self.algorithm_tuning,
        }

    def _refresh_effective_thresholds(self) -> None:
        effective = self._build_effective_thresholds(self._base_thresholds, self.algorithm_tuning)
        for key, value in effective.items():
            if hasattr(self.cfg.thresholds, key):
                setattr(self.cfg.thresholds, key, value)

    def _refresh_effective_thresholds_for(self, base: dict[str, Any]) -> None:
        effective = self._build_effective_thresholds(base, self.algorithm_tuning)
        for key, value in effective.items():
            if hasattr(self.cfg.thresholds, key):
                setattr(self.cfg.thresholds, key, value)

    def _build_effective_thresholds(self, base: dict[str, Any], tuning: str) -> dict[str, Any]:
        effective = deepcopy(base)

        def clamp(value: float, low: float, high: float) -> float:
            return max(low, min(high, value))

        def scale_limit(key: str, factor: float, floor: float = 0.0, ceiling: float = 25.0) -> None:
            effective[key] = clamp(float(base.get(key, effective.get(key, 0.0))) * factor, floor, ceiling)

        if tuning == "max_consumption":
            # Export threshold: raise the bar — battery-backed exports only occur at
            # higher FIT rates.  The battery stays full for home loads first.
            low = max(0.0, float(base.get("export_threshold_low", 0.0)) + 0.06)
            med = max(low + 0.03, float(base.get("export_threshold_medium", 0.0)) + 0.10)
            high = max(med + 0.05, float(base.get("export_threshold_high", 0.0)))
            effective["export_threshold_low"] = low
            effective["export_threshold_medium"] = med
            effective["export_threshold_high"] = high
            scale_limit("export_limit_low", 0.40, floor=0.3)
            scale_limit("export_limit_medium", 0.55, floor=0.8)
            scale_limit("export_limit_high", 0.75, floor=2.0)

            import_low = min(float(base.get("import_threshold_low", 0.0)) - 0.05, -0.01)
            import_med = min(float(base.get("import_threshold_medium", -0.15)) - 0.08, import_low - 0.05)
            import_high = min(float(base.get("import_threshold_high", -0.30)) - 0.10, import_med - 0.05)
            effective["import_threshold_low"] = import_low
            effective["import_threshold_medium"] = import_med
            effective["import_threshold_high"] = import_high
            scale_limit("import_limit_low", 0.60, floor=1.0)
            scale_limit("import_limit_medium", 0.75, floor=2.0)
            scale_limit("import_limit_high", 0.85, floor=4.0)

            effective["max_price_threshold"] = clamp(float(base.get("max_price_threshold", 0.10)) * 0.75, -1.0, 1.0)
            effective["cheap_import_price_threshold"] = clamp(float(base.get("cheap_import_price_threshold", 0.03)) * 0.60, -1.0, 1.0)
            effective["min_soc_floor"] = 12.5
            effective["midnight_reserve_soc"] = 80.0
            effective["morning_dump_target_soc"] = 12.5
            effective["sunrise_reserve_soc"] = clamp(float(base.get("sunrise_reserve_soc", 20.0)) + 10.0, 0.0, 100.0)
            effective["night_reserve_soc"] = clamp(float(base.get("night_reserve_soc", 15.0)) + 8.0, 0.0, 100.0)
            effective["daytime_topup_max_soc"] = clamp(float(base.get("daytime_topup_max_soc", 90.0)) + 5.0, 0.0, 100.0)
            effective["ess_first_discharge_pv_threshold_kw"] = clamp(float(base.get("ess_first_discharge_pv_threshold_kw", 1.0)) * 1.5, 0.2, 25.0)
            effective["allow_low_medium_export_positive_fit"] = False
            effective["allow_positive_fit_battery_discharging"] = False

            # --- Off-grid / energy-independence specific behaviours ---
            # Battery saturation export gate: only export to grid once battery is full.
            effective["battery_saturation_export_enabled"] = True
            effective["battery_saturation_export_soc"] = 98.0
            # Dynamic weather reserve: raise the floor automatically on storm/wind days.
            effective["dynamic_reserve_enabled"] = True
            # Morning space creation: if a big solar day is forecast, auto-enable morning
            # dump to make room in a full battery for the incoming generation.
            effective["morning_space_creation_enabled"] = True
            effective["morning_space_forecast_kwh"] = float(base.get("morning_space_forecast_kwh", 15.0))
            # Conditional grid import: only import when battery is low AND tomorrow's
            # solar won't recover it unaided.
            effective["conditional_grid_import_enabled"] = True
            effective["conditional_grid_import_solar_kwh"] = float(base.get("conditional_grid_import_solar_kwh", 5.0))
            effective["afternoon_lookahead_min_fraction"] = 0.85
            # Disable market-arbitrage and balanced-mode overrides.
            effective["forced_export_on_spike_enabled"] = False
            effective["forecast_hold_enabled"] = False
            effective["wacs_export_gate_enabled"] = False
            effective["evening_gap_reserve_enabled"] = False
            effective["variable_floor_enabled"] = False

        elif tuning == "max_profits":
            # Export threshold: lower the bar — export at smaller FIT rates to
            # maximise cycling revenue.
            low = max(0.0, float(base.get("export_threshold_low", 0.0)) - 0.04)
            med = max(low + 0.03, float(base.get("export_threshold_medium", 0.0)) - 0.06)
            high = max(med + 0.05, float(base.get("export_threshold_high", 0.0)) * 0.70)
            effective["export_threshold_low"] = low
            effective["export_threshold_medium"] = med
            effective["export_threshold_high"] = high
            scale_limit("export_limit_low", 1.35, floor=0.5)
            scale_limit("export_limit_medium", 1.25, floor=1.0)
            scale_limit("export_limit_high", 1.00, floor=2.0)

            import_low = float(base.get("import_threshold_low", 0.0)) + 0.05
            import_med = min(float(base.get("import_threshold_medium", -0.15)) + 0.08, import_low - 0.03)
            import_high = min(float(base.get("import_threshold_high", -0.30)) + 0.12, import_med - 0.03)
            effective["import_threshold_low"] = import_low
            effective["import_threshold_medium"] = import_med
            effective["import_threshold_high"] = import_high
            scale_limit("import_limit_low", 1.40, floor=1.0)
            scale_limit("import_limit_medium", 1.25, floor=2.0)
            scale_limit("import_limit_high", 1.00, floor=4.0)

            effective["max_price_threshold"] = clamp(float(base.get("max_price_threshold", 0.10)) + 0.05, -1.0, 1.0)
            # Accept cheap/near-zero imports aggressively to pre-fill for arbitrage.
            effective["cheap_import_price_threshold"] = clamp(float(base.get("cheap_import_price_threshold", 0.03)) + 0.04, -1.0, 1.0)
            # Floors set to hardware minimum so the battery can be fully cycled.
            effective["min_soc_floor"] = 1.0
            effective["midnight_reserve_soc"] = 50.0
            effective["morning_dump_target_soc"] = 1.0
            effective["sunrise_reserve_soc"] = clamp(float(base.get("sunrise_reserve_soc", 20.0)) - 6.0, 0.0, 100.0)
            effective["night_reserve_soc"] = clamp(float(base.get("night_reserve_soc", 15.0)) - 5.0, 0.0, 100.0)
            effective["daytime_topup_max_soc"] = clamp(float(base.get("daytime_topup_max_soc", 90.0)) - 5.0, 0.0, 100.0)
            effective["export_guard_relax_soc"] = clamp(float(base.get("export_guard_relax_soc", 90.0)) - 10.0, 0.0, 100.0)
            effective["ess_first_discharge_pv_threshold_kw"] = clamp(float(base.get("ess_first_discharge_pv_threshold_kw", 1.0)) * 0.60, 0.2, 25.0)
            effective["allow_low_medium_export_positive_fit"] = True
            effective["allow_positive_fit_battery_discharging"] = True

            # --- Market-arbitrage specific behaviours ---
            # Forced spike export: bypass SoC floor when FIT hits the spike threshold.
            effective["forced_export_on_spike_enabled"] = True
            effective["forced_export_spike_threshold"] = float(base.get("forced_export_spike_threshold", 1.00))
            # Forecast hold: hold battery at 100% when a high-price event is detected,
            # then fire max discharge when the event window opens.
            effective["forecast_hold_enabled"] = True
            effective["forecast_hold_price_threshold"] = float(base.get("forecast_hold_price_threshold", 0.50))
            effective["forecast_hold_start_hour"] = int(base.get("forecast_hold_start_hour", 14))
            effective["forecast_hold_end_hour"] = int(base.get("forecast_hold_end_hour", 22))
            effective["afternoon_lookahead_min_fraction"] = 0.97
            # Disable consumption and balanced-mode overrides.
            effective["battery_saturation_export_enabled"] = False
            effective["dynamic_reserve_enabled"] = False
            effective["morning_space_creation_enabled"] = False
            effective["conditional_grid_import_enabled"] = False
            effective["wacs_export_gate_enabled"] = False
            effective["evening_gap_reserve_enabled"] = False
            effective["variable_floor_enabled"] = False

        else:
            # balanced — smart hybrid / opportunity-cost behaviours
            effective["min_soc_floor"] = 2.5
            effective["midnight_reserve_soc"] = 70.0
            effective["morning_dump_target_soc"] = 2.5
            # WACS export gate: only discharge battery when FIT > cost of stored energy.
            effective["wacs_export_gate_enabled"] = True
            effective["wacs_buy_price"] = float(base.get("wacs_buy_price", 0.30))
            effective["wacs_round_trip_efficiency"] = float(base.get("wacs_round_trip_efficiency", 0.90))
            effective["wacs_degradation_cost_per_kwh"] = float(base.get("wacs_degradation_cost_per_kwh", 0.02))
            # Evening gap reserve: keep the battery charged to cover 6 PM–10 PM load.
            effective["evening_gap_reserve_enabled"] = True
            effective["evening_gap_start_hour"] = int(base.get("evening_gap_start_hour", 18))
            effective["evening_gap_end_hour"] = int(base.get("evening_gap_end_hour", 22))
            # Variable floor: rises from a low morning floor to a protective afternoon
            # floor, ensuring the battery is progressively reserved for evening.
            effective["variable_floor_enabled"] = True
            effective["variable_floor_morning_soc"] = float(base.get("variable_floor_morning_soc", 10.0))
            effective["variable_floor_afternoon_soc"] = float(base.get("variable_floor_afternoon_soc", 40.0))
            effective["variable_floor_morning_hour"] = int(base.get("variable_floor_morning_hour", 9))
            effective["variable_floor_afternoon_hour"] = int(base.get("variable_floor_afternoon_hour", 16))
            # Disable consumption and profits-mode overrides.
            effective["battery_saturation_export_enabled"] = False
            effective["dynamic_reserve_enabled"] = False
            effective["morning_space_creation_enabled"] = False
            effective["conditional_grid_import_enabled"] = False
            effective["forced_export_on_spike_enabled"] = False
            effective["forecast_hold_enabled"] = False

        # Apply per-profile user overrides from config.yaml [profiles:] section.
        # These run after all profile math so explicit config values always win.
        _profile_overrides = self.cfg.profile_overrides.get(tuning, {})
        if _profile_overrides:
            _valid_keys = {f.name for f in self.cfg.thresholds.__dataclass_fields__.values()}
            for key, value in _profile_overrides.items():
                if key in _valid_keys:
                    effective[key] = value
                else:
                    LOG.warning("profiles.%s: unknown threshold key %r — ignored", tuning, key)

        return effective

    def _persist_algorithm_tuning(self, tuning: str) -> None:
        try:
            self.state_store.set_json("algorithm_tuning", {"profile": tuning, "saved_at": self._now()})
        except Exception as exc:
            LOG.warning("Failed persisting algorithm tuning: %s", exc)

    def _restore_algorithm_tuning_on_startup(self) -> None:
        try:
            doc = self.state_store.get_json("algorithm_tuning") or {}
            tuning = str(doc.get("profile", "")).strip().lower()
            if tuning in ALGORITHM_TUNINGS:
                self.algorithm_tuning = tuning
                self._refresh_effective_thresholds()
                LOG.info("Restored persisted algorithm tuning on startup: %s", tuning)
        except Exception:
            LOG.exception("Failed restoring algorithm tuning")

    def set_algorithm_tuning(self, tuning: str, *, source: str = "api") -> dict[str, Any]:
        tuning = (tuning or "").strip().lower()
        if tuning not in ALGORITHM_TUNINGS:
            raise ValueError(f"Unsupported algorithm tuning: {tuning}")
        with self._lock:
            previous = self.algorithm_tuning
            self.algorithm_tuning = tuning
            self._refresh_effective_thresholds()
            self._sim_cache_block = None
        LOG.info("Action trigger (%s): set_algorithm_tuning %s -> %s", source, previous, tuning)
        self._persist_algorithm_tuning(tuning)
        return {
            "algorithm_tuning": self.algorithm_tuning,
            "thresholds": deepcopy(self.cfg.thresholds.__dict__),
            "base_thresholds": deepcopy(self._base_thresholds),
        }

    def _restore_daily_tuning_for_today(self) -> None:
        try:
            doc = self.state_store.get_json("daily_tuning") or {}
            day = str(doc.get("day", ""))
            today = datetime.now(self.tz).date().isoformat()
            if day != today:
                return
            params = doc.get("params") or {}
            if isinstance(params, dict) and params:
                self._apply_threshold_params(params)
                self._last_autotune_day = today
                self._autotune_summary = {
                    "day": day,
                    "applied": True,
                    "source": "restored",
                    "target_net": self.cfg.service.autotune_target_net,
                    "net_earnings": doc.get("net_earnings"),
                    "min_soc": doc.get("min_soc"),
                    "sunrise_soc": doc.get("sunrise_soc"),
                    "params": params,
                    "applied_at": self._now(),
                }
                LOG.info("Restored daily autotune profile for %s", day)
        except Exception:
            LOG.exception("Failed restoring daily tuning")

    def _maybe_run_daily_autotune(self) -> None:
        svc = self.cfg.service
        if not bool(getattr(svc, "enable_daily_autotune", True)):
            return
        now = datetime.now(self.tz)
        day = now.date().isoformat()
        if self._last_autotune_day == day:
            return
        run_hour = int(getattr(svc, "autotune_run_hour", 0))
        if now.hour < run_hour:
            return
        # Purge price tracking rows older than 7 days before running autotune.
        try:
            deleted = self.state_store.purge_old_price_tracking(retain_days=7)
            if deleted:
                LOG.info("Purged %d old price tracking rows (>7 days)", deleted)
        except Exception:
            LOG.debug("Failed to purge old price tracking rows", exc_info=True)
        self._run_daily_autotune(now)

    def _run_daily_autotune(self, now: datetime) -> None:
        base = deepcopy(self._base_thresholds)
        max_candidates = max(8, int(getattr(self.cfg.service, "autotune_max_candidates", 80)))
        target_net = float(getattr(self.cfg.service, "autotune_target_net", 2.0))
        min_soc_floor_hard = float(getattr(self.cfg.service, "autotune_min_soc_hard_floor", 2.0))
        min_sunrise_soc = float(getattr(self.cfg.service, "autotune_min_sunrise_soc", 8.0))

        low_profiles: list[tuple[float, float, float]] = [
            (0.05, 0.10, 0.25),
            (0.06, 0.12, 0.25),
            (0.08, 0.15, 0.30),
            (0.10, 0.20, 1.00),
        ]
        base_low = float(base.get("export_threshold_low", 0.1))
        base_med = float(base.get("export_threshold_medium", 0.2))
        base_high = float(base.get("export_threshold_high", 1.0))
        if (base_low, base_med, base_high) not in low_profiles:
            low_profiles.insert(0, (base_low, base_med, base_high))

        # Build candidate ranges around configured safety floors so aggressive
        # profiles (e.g. 2.5%) are actually reachable by autotune.
        base_min_soc = float(base.get("min_soc_floor", min_soc_floor_hard))
        base_sunrise_soc = float(base.get("sunrise_reserve_soc", min_sunrise_soc))
        base_midnight_soc = float(base.get("midnight_reserve_soc", 70.0))

        min_soc_opts = sorted(
            {
                round(min_soc_floor_hard, 2),
                round(min_soc_floor_hard + 0.5, 2),
                round(min_soc_floor_hard + 1.0, 2),
                round(min_soc_floor_hard + 1.5, 2),
                round(min_soc_floor_hard + 2.5, 2),
                round(min_soc_floor_hard + 4.0, 2),
                round(base_min_soc, 2),
            }
        )
        sunrise_opts = sorted(
            {
                round(min_sunrise_soc, 2),
                round(min_sunrise_soc + 0.5, 2),
                round(min_sunrise_soc + 1.0, 2),
                round(min_sunrise_soc + 1.5, 2),
                round(min_sunrise_soc + 2.5, 2),
                round(min_sunrise_soc + 4.0, 2),
                round(base_sunrise_soc, 2),
            }
        )
        buffer_opts = [0.0, 0.5, 1.0, 1.5, 2.0]

        # midnight_reserve_soc is a user-configured safety floor, not an optimization
        # target.  Autotune may explore higher values (more conservative) but must
        # never go below what the user has explicitly set — doing so defeats the
        # purpose of the midnight protection entirely.
        midnight_soc_opts = sorted({
            round(base_midnight_soc, 1),
            round(min(100.0, base_midnight_soc + 5.0), 1),
            round(min(100.0, base_midnight_soc + 10.0), 1),
        })

        candidates: list[dict[str, Any]] = []
        for mn in min_soc_opts:
            for sr in sunrise_opts:
                for sb in buffer_opts:
                    for low, med, high in low_profiles:
                        if not (low < med < high):
                            continue
                        for midnight_soc in midnight_soc_opts:
                            candidates.append(
                                {
                                    "min_soc_floor": float(mn),
                                    "sunrise_reserve_soc": float(sr),
                                    "sunrise_buffer_percent": float(sb),
                                    "export_threshold_low": float(low),
                                    "export_threshold_medium": float(med),
                                    "export_threshold_high": float(high),
                                    "export_limit_high": min(25.0, float(base.get("export_limit_high", 25.0))),
                                    "morning_dump_enabled": True,
                                    "midnight_reserve_soc": float(midnight_soc),
                                }
                            )
        if len(candidates) > max_candidates:
            # Evenly sample across the full search space instead of taking the
            # first N, which can bias outcomes by loop ordering.
            sampled: list[dict[str, Any]] = []
            step = len(candidates) / float(max_candidates)
            for i in range(max_candidates):
                idx = int(i * step)
                if idx >= len(candidates):
                    idx = len(candidates) - 1
                sampled.append(candidates[idx])
            dedup: list[dict[str, Any]] = []
            seen: set[tuple[tuple[str, Any], ...]] = set()
            for c in sampled:
                key = tuple(sorted(c.items()))
                if key in seen:
                    continue
                seen.add(key)
                dedup.append(c)
            candidates = dedup

        prices = self.prices_snapshot()
        sunrise_dt = self._parse_iso_ts((prices.get("sun") or {}).get("sunrise"))
        if sunrise_dt:
            sunrise_dt = sunrise_dt.astimezone(self.tz)

        best: dict[str, Any] | None = None
        best_score = float("-inf")
        tested = 0
        rejected_floor = 0
        rejected_sunrise = 0
        rejected_empty = 0
        best_observed_net = float("-inf")
        best_observed_min_soc = 0.0
        best_observed_sunrise_soc = 0.0

        try:
            for params in candidates:
                tested += 1
                candidate_base = deepcopy(base)
                for key, value in params.items():
                    candidate_base[key] = value
                self._refresh_effective_thresholds_for(candidate_base)
                self.optimizer._export_hysteresis_on = False
                sim = self.simulate_automated(log_summary=False, context="autotune")
                net = float(sim.get("net_earnings", 0.0))
                series = sim.get("series") or []
                if not series:
                    rejected_empty += 1
                    continue
                min_soc = min(float(p.get("soc", 0.0)) for p in series)
                sunrise_soc = min_soc
                if sunrise_dt:
                    near = min(
                        series,
                        key=lambda p: abs((datetime.fromisoformat(str(p.get("time"))) - sunrise_dt).total_seconds()),
                    )
                    sunrise_soc = float(near.get("soc", min_soc))
                if net > best_observed_net:
                    best_observed_net = net
                    best_observed_min_soc = min_soc
                    best_observed_sunrise_soc = sunrise_soc
                if min_soc < min_soc_floor_hard:
                    rejected_floor += 1
                    continue

                if sunrise_soc < min_sunrise_soc:
                    rejected_sunrise += 1
                    continue

                # Primary objective: maximize net earnings.
                # Tie-breakers: closer to target and higher sunrise SoC.
                score = net * 1000.0 - abs(net - target_net) * 10.0 + sunrise_soc
                if score > best_score:
                    best_score = score
                    best = {
                        "params": deepcopy(params),
                        "net_earnings": net,
                        "min_soc": min_soc,
                        "sunrise_soc": sunrise_soc,
                    }
        finally:
            self._base_thresholds = deepcopy(base)
            self._refresh_effective_thresholds()
            self.optimizer._export_hysteresis_on = False

        day = now.date().isoformat()
        if best:
            for key, value in best["params"].items():
                self._base_thresholds[key] = value
            self._refresh_effective_thresholds()
            self._last_autotune_day = day
            self._autotune_summary = {
                "day": day,
                "applied": True,
                "source": "midnight",
                "tested": tested,
                "target_net": target_net,
                "net_earnings": round(float(best["net_earnings"]), 4),
                "min_soc": round(float(best["min_soc"]), 2),
                "sunrise_soc": round(float(best["sunrise_soc"]), 2),
                "params": best["params"],
                "applied_at": self._now(),
            }
            try:
                self.state_store.set_json(
                    "daily_tuning",
                    {
                        "day": day,
                        "params": best["params"],
                        "net_earnings": best["net_earnings"],
                        "min_soc": best["min_soc"],
                        "sunrise_soc": best["sunrise_soc"],
                        "tested": tested,
                        "saved_at": self._now(),
                    },
                )
            except Exception as exc:
                LOG.warning("Failed persisting daily tuning: %s", exc)
            LOG.info(
                "Daily autotune applied (%s): tested=%d net=%.3f min_soc=%.2f sunrise_soc=%.2f params=%s",
                day,
                tested,
                float(best["net_earnings"]),
                float(best["min_soc"]),
                float(best["sunrise_soc"]),
                best["params"],
            )
            return

        self._last_autotune_day = day
        self._autotune_summary = {
            "day": day,
            "applied": False,
            "source": "midnight",
            "tested": tested,
            "target_net": target_net,
            "reason": "No safe candidate found",
            "applied_at": self._now(),
        }
        if best_observed_net == float("-inf"):
            LOG.warning(
                "Daily autotune found no safe candidate for %s (tested=%d, empty=%d)",
                day,
                tested,
                rejected_empty,
            )
            return
        LOG.warning(
            "Daily autotune found no safe candidate for %s (tested=%d, rejected_floor=%d, rejected_sunrise=%d, empty=%d, best_observed_net=%.3f, best_observed_min_soc=%.2f, best_observed_sunrise_soc=%.2f)",
            day,
            tested,
            rejected_floor,
            rejected_sunrise,
            rejected_empty,
            best_observed_net,
            best_observed_min_soc,
            best_observed_sunrise_soc,
        )

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
        last_ts = OptimizerRuntime._parse_iso_ts(last.get("time"))
        if not last_ts:
            return points
        target = day_end.isoformat()
        if last_ts >= day_end or str(last.get("time")) == target:
            return points
        out = list(points)
        out.append({"time": target, "value": last.get("value"), "kind": "forecast"})
        return out

    def prices_snapshot(self) -> dict[str, Any]:
        states = self.client.get_all_states()
        e = self.cfg.entities
        now = datetime.now(self.tz)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
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

            sunrise_dt: datetime | None = None
            sunset_dt: datetime | None = None
            if next_rising and next_setting:
                if is_sun_up:
                    # Daytime: next rising is tomorrow, next setting is today's sunset.
                    sunrise_dt = next_rising - timedelta(days=1)
                    sunset_dt = next_setting
                else:
                    if next_rising.date() == now.date():
                        # Pre-sunrise
                        sunrise_dt = next_rising
                        sunset_dt = next_setting
                    else:
                        # Post-sunset: both next_* are tomorrow; shift back for today's curve.
                        sunrise_dt = next_rising - timedelta(days=1)
                        sunset_dt = next_setting - timedelta(days=1)

            if sunrise_dt and sunset_dt:
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
                "current": self._to_float(import_item.state if import_item else None),
                "points": import_points,
            },
            "export": {
                "entity_id": e.feedin_sensor,
                "forecast_entity_id": export_forecast_entity,
                "history_entity_id": export_history_entity,
                "current": self._to_float(export_item.state if export_item else None),
                "points": export_points,
            },
            "pv_forecast": {
                "entity_id": e.forecast_today_sensor,
                "total_kwh": pv_total,
                "points": pv_points,
            },
        }

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

    def power_snapshot(self) -> dict[str, Any]:
        e = self.cfg.entities
        now = datetime.now(self.tz)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)

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
                    fill_flat_if_empty=True,
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
                    fill_flat_if_empty=True,
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
            "sun": self.prices_snapshot().get("sun", {}),
        }

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

    def simulate_automated(
        self,
        *,
        log_summary: bool = True,
        context: str = "manual",
        apply_now: bool = False,
        tuning_override: str | None = None,
    ) -> dict[str, Any]:
        cfg = deepcopy(self.cfg) if tuning_override else self.cfg
        if tuning_override:
            tuning_override = (tuning_override or "").strip().lower()
            if tuning_override not in ALGORITHM_TUNINGS:
                raise ValueError(f"Unsupported algorithm tuning: {tuning_override}")
            effective = self._build_effective_thresholds(self._base_thresholds, tuning_override)
            for key, value in effective.items():
                if hasattr(cfg.thresholds, key):
                    setattr(cfg.thresholds, key, value)
            optimizer = Optimizer(cfg, self.client, timezone=self.timezone)
        else:
            optimizer = self.optimizer
        # Keep simulation isolated from live optimizer hysteresis state so
        # repeated runs are deterministic for the same inputs.
        original_export_hysteresis = optimizer._export_hysteresis_on
        optimizer._export_hysteresis_on = False
        # When apply_now=True, track the decision and hysteresis state at the
        # step that corresponds to the current wall-clock time.  The simulation
        # evolves SoC from start-of-day using real energy flows, so this
        # decision is better-informed than a single-step reactive computation.
        current_decision: Decision | None = None
        current_hysteresis: bool = False

        e = cfg.entities
        prices = self.prices_snapshot()
        now = datetime.now(self.tz)
        day_start = self._parse_iso_ts(prices.get("day_start")) or now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = self._parse_iso_ts(prices.get("day_end")) or (now + timedelta(hours=12))
        all_states = self.client.get_all_states()

        def _to_timed_map(points: list[dict[str, Any]]) -> list[tuple[datetime, float]]:
            out: list[tuple[datetime, float]] = []
            for p in points:
                ts = self._parse_iso_ts(p.get("time"))
                v = self._to_float(p.get("value"))
                if not ts or v is None:
                    continue
                out.append((ts.astimezone(self.tz), float(v)))
            out.sort(key=lambda x: x[0])
            return out

        def _value_at(series: list[tuple[datetime, float]], at: datetime, default: float = 0.0) -> float:
            if not series:
                return default
            # Binary search: find rightmost entry with ts <= at
            idx = bisect_right(series, (at, float("inf"))) - 1
            if idx < 0:
                return series[0][1]
            return series[idx][1]

        def _future_sum(series: list[tuple[datetime, float]], at: datetime) -> float:
            return sum(v for ts, v in series if ts >= at)

        import_series = _to_timed_map(prices.get("import", {}).get("points", []))
        export_series = _to_timed_map(prices.get("export", {}).get("points", []))
        pv_series = _to_timed_map(prices.get("pv_forecast", {}).get("points", []))
        load_series = [
            (self._parse_iso_ts(p.get("time")).astimezone(self.tz), float(p.get("value")))
            for p in self._extract_history_series(
                entity_id=e.consumed_power_sensor,
                day_start=day_start,
                now=now,
                to_kw=True,
                floor_zero=True,
                fill_flat_if_empty=True,
            )
            if self._parse_iso_ts(p.get("time")) and self._to_float(p.get("value")) is not None
        ]
        load_series.sort(key=lambda x: x[0])
        daily_avg_load_kw = 0.0
        if load_series:
            daily_avg_load_kw = max(0.0, sum(v for _, v in load_series) / len(load_series))
        else:
            daily_avg_load_kw = max(
                0.0,
                self._to_float(all_states.get(e.consumed_power_sensor).state if all_states.get(e.consumed_power_sensor) else 0.0)
                or 0.0,
            )
        # Evening load estimate: average of midnight-to-6am history, plus 25% uplift.
        # This accounts for the higher uncertainty in evening consumption vs. the quiet
        # overnight baseline, preventing the simulation from over-discharging into evening.
        overnight_vals = [v for ts_pt, v in load_series if ts_pt.hour < 6]
        if overnight_vals:
            evening_load_kw = (sum(overnight_vals) / len(overnight_vals)) * 1.25
        else:
            evening_load_kw = daily_avg_load_kw
        soc_series = [
            (self._parse_iso_ts(p.get("time")).astimezone(self.tz), float(p.get("value")))
            for p in self._extract_history_series(
                entity_id=e.battery_soc_sensor,
                day_start=day_start,
                now=now,
                to_kw=False,
                floor_zero=True,
                fill_flat_if_empty=True,
            )
            if self._parse_iso_ts(p.get("time")) and self._to_float(p.get("value")) is not None
        ]
        soc_series.sort(key=lambda x: x[0])

        # Live SoC seeds the simulation at the transition from past → future steps.
        live_soc_raw = self._to_float(
            all_states.get(e.battery_soc_sensor).state
            if all_states.get(e.battery_soc_sensor)
            else None
        )
        if live_soc_raw is None and soc_series:
            live_soc_raw = soc_series[-1][1]

        timeline = sorted(
            {
                ts
                for ts, _ in import_series + export_series + pv_series
                if ts >= day_start and ts <= day_end
            }
        )
        if not timeline:
            timeline = [day_start, day_end]
        elif timeline[0] > day_start:
            timeline.insert(0, day_start)
        if timeline[-1] < day_end:
            timeline.append(day_end)

        cap_raw = self._to_float((all_states.get(e.rated_capacity_sensor).state if all_states.get(e.rated_capacity_sensor) else None))
        cap_uom = str((all_states.get(e.rated_capacity_sensor).attributes if all_states.get(e.rated_capacity_sensor) else {}).get("unit_of_measurement", "kWh")).lower()
        cap_kwh = (cap_raw / 1000.0) if (cap_raw and cap_uom == "wh") else (cap_raw or 20.0)
        cap_kwh = max(1.0, cap_kwh)
        min_soc_floor = max(0.0, min(100.0, float(getattr(cfg.thresholds, "min_soc_floor", 0.0))))
        initial_soc = soc_series[0][1] if soc_series else live_soc_raw
        soc = max(0.0, min(100.0, initial_soc if initial_soc is not None else 50.0))

        sun = prices.get("sun", {})
        sunrise = self._parse_iso_ts(sun.get("sunrise"))
        sunset = self._parse_iso_ts(sun.get("sunset"))
        if sunrise:
            sunrise = sunrise.astimezone(self.tz)
        if sunset:
            sunset = sunset.astimezone(self.tz)

        def _sun_state_for(at: datetime) -> EntityState:
            attrs = dict((all_states.get(e.sun_entity).attributes if all_states.get(e.sun_entity) else {}) or {})
            if sunrise and sunset:
                if at < sunrise:
                    attrs["next_rising"] = sunrise.isoformat()
                    attrs["next_setting"] = sunset.isoformat()
                    state = "below_horizon"
                elif at < sunset:
                    attrs["next_rising"] = (sunrise + timedelta(days=1)).isoformat()
                    attrs["next_setting"] = sunset.isoformat()
                    state = "above_horizon"
                else:
                    attrs["next_rising"] = (sunrise + timedelta(days=1)).isoformat()
                    attrs["next_setting"] = (sunset + timedelta(days=1)).isoformat()
                    state = "below_horizon"
            else:
                state = "unknown"
            return EntityState(entity_id=e.sun_entity, state=state, attributes=attrs)

        simulated_points: list[dict[str, Any]] = []
        mode_points: list[dict[str, Any]] = []
        total_import_earnings = 0.0
        total_export_earnings = 0.0
        total_import_kwh = 0.0
        total_export_kwh = 0.0
        eff = 0.96  # Round-trip battery efficiency applied to charge/discharge deltas
        _live_soc_seeded = False
        LOG.debug(
            "simulate_automated (%s): cap=%.1fkWh initial_soc=%.1f%% live_soc=%s "
            "timeline=%d steps load_avg=%.2fkW day=%s\u2013%s",
            context, cap_kwh,
            initial_soc if initial_soc is not None else soc,
            f"{live_soc_raw:.1f}%" if live_soc_raw is not None else "n/a",
            len(timeline),
            daily_avg_load_kw,
            day_start.strftime("%Y-%m-%d %H:%M"),
            day_end.strftime("%H:%M"),
        )
        try:
            for idx, ts in enumerate(timeline[:-1]):
                nxt = timeline[idx + 1]
                dt_hours = max(1.0 / 60.0, (nxt - ts).total_seconds() / 3600.0)

                # For past steps replace accumulated SoC with the actual recorded value
                # so the chart shows what really happened rather than a re-simulation.
                # At the first future step, seed from the live reading so the forward
                # projection is anchored to reality, not the replayed midnight SoC.
                if ts <= now and soc_series:
                    soc = _value_at(soc_series, ts, soc)
                elif ts > now and not _live_soc_seeded:
                    soc = max(0.0, min(100.0, live_soc_raw if live_soc_raw is not None else soc))
                    _live_soc_seeded = True

                sim_states = dict(all_states)
                import_price = _value_at(import_series, ts, self._to_float(all_states.get(e.price_sensor).state if all_states.get(e.price_sensor) else 0.0) or 0.0)
                export_price = _value_at(export_series, ts, self._to_float(all_states.get(e.feedin_sensor).state if all_states.get(e.feedin_sensor) else 0.0) or 0.0)
                pv_kw = max(0.0, _value_at(pv_series, ts, 0.0))
                # Use time-varying load from historical data where available; fall
                # back to daily average, except after 7pm where an overnight-derived
                # estimate (midnight-6am avg +25%) is used to avoid over-discharging.
                if ts.hour >= 19:
                    load_kw = evening_load_kw
                else:
                    load_kw = _value_at(load_series, ts, daily_avg_load_kw)

                # Force estimate=False so that price_is_actual reflects the simulated price
                # values rather than the live sensor's current estimate flag, which can be
                # True during forecast periods and would suppress all negative-price import.
                price_attrs = dict((all_states.get(e.price_sensor).attributes if all_states.get(e.price_sensor) else {}) or {})
                price_attrs["estimate"] = False
                sim_states[e.price_sensor] = EntityState(e.price_sensor, str(import_price), price_attrs)
                feedin_attrs = dict((all_states.get(e.feedin_sensor).attributes if all_states.get(e.feedin_sensor) else {}) or {})
                feedin_attrs["estimate"] = False
                # Inject future sim prices as the forecast attribute so look-ahead
                # logic in _compute sees simulated prices rather than stale live data.
                feedin_attrs["forecast"] = [
                    {"time": t_pt.isoformat(), "value": float(v)}
                    for t_pt, v in export_series
                    if t_pt > ts
                ]
                sim_states[e.feedin_sensor] = EntityState(e.feedin_sensor, str(export_price), feedin_attrs)
                # Derive price_spike from simulated import price so it isn't frozen at
                # whatever the real sensor happens to be for every interval.
                sim_spike_state = "on" if import_price >= cfg.thresholds.export_threshold_high else "off"
                sim_states[e.price_spike_sensor] = EntityState(e.price_spike_sensor, sim_spike_state, {})
                # Derive negative_price_expected from upcoming import series when configured.
                if e.negative_price_expected_sensor:
                    future_neg = any(v < 0 for t_pt, v in import_series if t_pt > ts)
                    sim_states[e.negative_price_expected_sensor] = EntityState(
                        e.negative_price_expected_sensor,
                        "on" if future_neg else "off",
                        {},
                    )
                # Weather entity: preserve the live reading for past steps; for future
                # steps inject a "clear" condition so the weather-based reserve does not
                # inflate simulation floors beyond what today's actual forecast shows.
                if e.weather_entity and ts > now:
                    sim_states[e.weather_entity] = EntityState(
                        e.weather_entity, "clear", {"forecast": []}
                    )
                sim_states[e.pv_power_sensor] = EntityState(e.pv_power_sensor, str(pv_kw), dict((all_states.get(e.pv_power_sensor).attributes if all_states.get(e.pv_power_sensor) else {}) or {}))
                sim_states[e.consumed_power_sensor] = EntityState(e.consumed_power_sensor, str(load_kw), dict((all_states.get(e.consumed_power_sensor).attributes if all_states.get(e.consumed_power_sensor) else {}) or {}))
                sim_states[e.battery_soc_sensor] = EntityState(e.battery_soc_sensor, str(soc), dict((all_states.get(e.battery_soc_sensor).attributes if all_states.get(e.battery_soc_sensor) else {}) or {}))
                sim_states[e.available_discharge_sensor] = EntityState(e.available_discharge_sensor, str(round(soc * 0.01 * cap_kwh, 3)), dict((all_states.get(e.available_discharge_sensor).attributes if all_states.get(e.available_discharge_sensor) else {}) or {}))
                sim_states[e.forecast_remaining_sensor] = EntityState(
                    e.forecast_remaining_sensor,
                    str(max(0.0, _future_sum(pv_series, ts))),
                    dict((all_states.get(e.forecast_remaining_sensor).attributes if all_states.get(e.forecast_remaining_sensor) else {}) or {}),
                )
                sim_states[e.sun_entity] = _sun_state_for(ts)

                d = optimizer._compute(sim_states, now_dt=ts)
                # Capture the decision for the most-recent historical step so
                # apply_now has a simulation-informed decision to push to hardware.
                if ts <= now:
                    current_decision = d
                    current_hysteresis = optimizer._export_hysteresis_on
                # Start with raw forecast PV, then apply curtailment if battery headroom is exhausted.
                sim_pv_output_kw = pv_kw
                sim_pv_curtailed_kw = 0.0
                sim_import_kw = float(d.desired_import_limit)
                sim_export_kw = float(d.desired_export_limit)
                import_kwh = max(0.0, sim_import_kw * dt_hours)
                export_kwh = max(0.0, sim_export_kw * dt_hours)
                # Import "earnings" are positive when price is negative (paid to import),
                # negative when paying for imports.
                import_earnings = -import_kwh * import_price
                export_earnings = export_kwh * export_price
                total_import_kwh += import_kwh
                total_export_kwh += export_kwh
                total_import_earnings += import_earnings
                total_export_earnings += export_earnings
                simulated_points.append(
                    {
                        "time": ts.isoformat(),
                        "import_kw": float(sim_import_kw),
                        "export_kw": float(sim_export_kw),
                        "consumption_kw": float(load_kw),
                        "pv_forecast_kw": float(pv_kw),
                        "pv_output_kw": float(sim_pv_output_kw),
                        "pv_curtailed_kw": float(sim_pv_curtailed_kw),
                        "import_price": float(import_price),
                        "export_price": float(export_price),
                        "import_earnings": float(import_earnings),
                        "export_earnings": float(export_earnings),
                        "cum_import_earnings": float(total_import_earnings),
                        "cum_export_earnings": float(total_export_earnings),
                        "soc": float(soc),
                    }
                )
                mode_points.append({"time": ts.isoformat(), "mode": d.desired_mode})

                net_batt_kw = (sim_pv_output_kw - load_kw) + sim_import_kw - sim_export_kw
                headroom_kwh = max(0.0, (100.0 - soc) * 0.01 * cap_kwh)
                max_charge_kw = headroom_kwh / max(1e-6, dt_hours * eff)
                if net_batt_kw > max_charge_kw:
                    overflow_kw = net_batt_kw - max_charge_kw
                    sim_pv_curtailed_kw = max(0.0, min(sim_pv_output_kw, overflow_kw))
                    sim_pv_output_kw = max(0.0, sim_pv_output_kw - sim_pv_curtailed_kw)
                    # When battery is full and PV is curtailed, import cannot exceed load.
                    if soc >= 99.9 and sim_pv_curtailed_kw > 0:
                        sim_import_kw = min(sim_import_kw, max(0.0, load_kw))
                        simulated_points[-1]["import_kw"] = float(sim_import_kw)
                        import_kwh = max(0.0, sim_import_kw * dt_hours)
                        import_earnings = -import_kwh * import_price
                        total_import_kwh -= max(0.0, float(d.desired_import_limit) * dt_hours)
                        total_import_earnings -= -max(0.0, float(d.desired_import_limit) * dt_hours) * import_price
                        total_import_kwh += import_kwh
                        total_import_earnings += import_earnings
                        simulated_points[-1]["import_earnings"] = float(import_earnings)
                        simulated_points[-1]["cum_import_earnings"] = float(total_import_earnings)
                    net_batt_kw = (sim_pv_output_kw - load_kw) + sim_import_kw - sim_export_kw
                    simulated_points[-1]["pv_output_kw"] = float(sim_pv_output_kw)
                    simulated_points[-1]["pv_curtailed_kw"] = float(sim_pv_curtailed_kw)

                # Enforce the effective SoC floor as a hard simulation bound.
                # Use the dynamic export_floor_soc from the decision (which includes the
                # midnight reserve forward-looking floor) so a single large-step export
                # cannot overshoot the same floor that _compute() used to gate exports.
                # Exception: during morning dump, _compute() bypasses the regular floor
                # (including midnight reserve which can reach 100% in the pre-dawn hours)
                # and intentionally drains to morning_dump_target_soc (= min_soc_floor).
                # Using export_floor_soc here would cancel every simulated dump step.
                step_floor = min_soc_floor if d.morning_dump_active else max(min_soc_floor, d.export_floor_soc)
                usable_discharge_kwh = max(0.0, (soc - step_floor) * 0.01 * cap_kwh)
                max_discharge_kw = usable_discharge_kwh * eff / max(1e-6, dt_hours)
                if net_batt_kw < -max_discharge_kw:
                    needed_kw = (-max_discharge_kw) - net_batt_kw
                    reduce_export_kw = min(max(0.0, sim_export_kw), max(0.0, needed_kw))
                    if reduce_export_kw > 0:
                        old_export_kwh = max(0.0, sim_export_kw * dt_hours)
                        old_export_earnings = old_export_kwh * export_price
                        sim_export_kw -= reduce_export_kw
                        export_kwh = max(0.0, sim_export_kw * dt_hours)
                        export_earnings = export_kwh * export_price
                        total_export_kwh += export_kwh - old_export_kwh
                        total_export_earnings += export_earnings - old_export_earnings
                        needed_kw -= reduce_export_kw
                    if needed_kw > 0 and import_price < 0:
                        old_import_kwh = max(0.0, sim_import_kw * dt_hours)
                        old_import_earnings = -old_import_kwh * import_price
                        sim_import_kw += needed_kw
                        import_kwh = max(0.0, sim_import_kw * dt_hours)
                        import_earnings = -import_kwh * import_price
                        total_import_kwh += import_kwh - old_import_kwh
                        total_import_earnings += import_earnings - old_import_earnings

                    net_batt_kw = (sim_pv_output_kw - load_kw) + sim_import_kw - sim_export_kw
                    simulated_points[-1]["import_kw"] = float(sim_import_kw)
                    simulated_points[-1]["export_kw"] = float(sim_export_kw)
                    simulated_points[-1]["import_earnings"] = float(-max(0.0, sim_import_kw * dt_hours) * import_price)
                    simulated_points[-1]["export_earnings"] = float(max(0.0, sim_export_kw * dt_hours) * export_price)
                    simulated_points[-1]["cum_import_earnings"] = float(total_import_earnings)
                    simulated_points[-1]["cum_export_earnings"] = float(total_export_earnings)
                delta = (net_batt_kw * dt_hours / cap_kwh) * 100.0
                soc = soc + (delta * eff if delta >= 0 else delta / eff)
                # Clamp to the absolute physical minimum only.  The export floor
                # (step_floor, which may include the midnight reserve) is an economic
                # guard used above to limit exports/discharge; it must NOT be applied
                # here as a physical SoC minimum.  Only the absolute min is enforced.
                # Example: at 04:30 with no PV, midnight reserve drives export_floor
                # to 100%, but the battery physically drains for load consumption and
                # must be allowed to continue declining below that economic floor.
                soc = max(min_soc_floor, min(100.0, soc))
        finally:
            if apply_now and current_decision is not None:
                # Persist the hysteresis state the simulation reached at 'now'
                # so the live controller continues from a consistent state.
                optimizer._export_hysteresis_on = current_hysteresis
            else:
                optimizer._export_hysteresis_on = original_export_hysteresis

        if apply_now and current_decision is not None:
            # Re-fetch live HA states so _apply change-detection is accurate.
            live_states = self.client.get_all_states()
            # Re-compute using live states rather than applying the simulation's
            # historical decision directly.  The simulation's "current step" SoC
            # is from the last price-interval history record (can be several
            # minutes stale) and can miss SoC boundaries crossed since then —
            # most critically the midnight_reserve hard-stop.
            live_decision = optimizer._compute(live_states)
            if (live_decision.desired_export_limit != current_decision.desired_export_limit
                    or live_decision.desired_mode != current_decision.desired_mode):
                LOG.info(
                    "Simulation apply_now (%s): live recompute changed decision "
                    "(mode %s→%s export %.2f→%.2f soc sim=%.1f%% live=%.1f%%)",
                    context,
                    current_decision.desired_mode, live_decision.desired_mode,
                    current_decision.desired_export_limit, live_decision.desired_export_limit,
                    current_decision.battery_soc, live_decision.battery_soc,
                )
            current_decision = live_decision
            optimizer._apply(live_states, current_decision)
            optimizer._send_summaries(live_states, current_decision)
            LOG.info(
                "Simulation apply_now (%s): mode=%s export=%.2f import=%.2f soc=%.2f reason=%s",
                context,
                current_decision.desired_mode,
                current_decision.desired_export_limit,
                current_decision.desired_import_limit,
                current_decision.battery_soc,
                current_decision.reason,
            )

        result = {
            "generated_at": now.isoformat(),
            "day_end": day_end.isoformat(),
            "algorithm_tuning": tuning_override or self.algorithm_tuning,
            "daily_average_load_kw": float(daily_avg_load_kw),
            "import_kwh": float(total_import_kwh),
            "export_kwh": float(total_export_kwh),
            "import_earnings": float(total_import_earnings),
            "export_earnings": float(total_export_earnings),
            "net_earnings": float(total_import_earnings + total_export_earnings),
            "series": simulated_points,
            "modes": mode_points,
        }
        if apply_now and current_decision is not None:
            result["applied_decision"] = asdict(current_decision)
        if log_summary:
            min_soc = min((float(p.get("soc", 100.0)) for p in simulated_points), default=100.0)
            max_soc = max((float(p.get("soc", 0.0)) for p in simulated_points), default=0.0)
            LOG.info(
                "Simulation summary (%s): net=%.3f import=%.2fkWh export=%.2fkWh import_$=%.3f export_$=%.3f min_soc=%.2f max_soc=%.2f points=%d",
                context,
                float(result["net_earnings"]),
                float(result["import_kwh"]),
                float(result["export_kwh"]),
                float(result["import_earnings"]),
                float(result["export_earnings"]),
                min_soc,
                max_soc,
                len(simulated_points),
            )
        return result

    def simulate_tuning_comparison(self, *, passes: int = 8, context: str = "api") -> dict[str, Any]:
        passes = max(1, min(20, int(passes)))
        results: list[dict[str, Any]] = []
        for tuning in sorted(ALGORITHM_TUNINGS):
            best_sim: dict[str, Any] | None = None
            best_net = float("-inf")
            best_pass = 0
            for idx in range(1, passes + 1):
                sim = self.simulate_automated(
                    log_summary=False,
                    context=f"{context}:{tuning}:{idx}",
                    tuning_override=tuning,
                )
                net = float(sim.get("net_earnings", float("-inf")))
                if best_sim is None or net > best_net:
                    best_sim = sim
                    best_net = net
                    best_pass = idx
            if best_sim is None:
                continue
            series = best_sim.get("series") or []
            min_soc = min((float(p.get("soc", 100.0)) for p in series), default=100.0)
            max_soc = max((float(p.get("soc", 0.0)) for p in series), default=0.0)
            results.append(
                {
                    "tuning": tuning,
                    "best_pass": best_pass,
                    "simulation": best_sim,
                    "summary": {
                        "net_earnings": float(best_sim.get("net_earnings", 0.0)),
                        "import_earnings": float(best_sim.get("import_earnings", 0.0)),
                        "export_earnings": float(best_sim.get("export_earnings", 0.0)),
                        "import_kwh": float(best_sim.get("import_kwh", 0.0)),
                        "export_kwh": float(best_sim.get("export_kwh", 0.0)),
                        "min_soc": float(min_soc),
                        "max_soc": float(max_soc),
                    },
                }
            )
        results.sort(key=lambda item: float(item.get("summary", {}).get("net_earnings", float("-inf"))), reverse=True)
        return {
            "generated_at": self._now(),
            "current_tuning": self.algorithm_tuning,
            "passes": passes,
            "results": results,
        }
