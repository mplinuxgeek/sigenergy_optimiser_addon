# Part of OptimizerRuntime — see optimizer/runtime/__init__.py
from __future__ import annotations

import logging
import time
from dataclasses import asdict
from datetime import datetime
from typing import Any, Callable

from optimizer.controller import Decision

LOG = logging.getLogger(__name__)


class _LifecycleMixin:
    def add_cycle_listener(self, callback: Callable[[], None]) -> None:
        """Register a callback to be invoked (from the worker thread) after each cycle."""
        self._cycle_listeners.append(callback)

    def remove_cycle_listener(self, callback: Callable[[], None]) -> None:
        try:
            self._cycle_listeners.remove(callback)
        except ValueError:
            pass

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
            self._compute_and_store_fit_windows()

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
