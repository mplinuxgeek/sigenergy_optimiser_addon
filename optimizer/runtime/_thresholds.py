# Part of OptimizerRuntime — see optimizer/runtime/__init__.py
from __future__ import annotations

import logging
from copy import deepcopy
from datetime import datetime
from typing import Any

from optimizer.runtime._constants import ALGORITHM_TUNINGS

LOG = logging.getLogger(__name__)


class _ThresholdsMixin:
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
        should_force_cycle = False
        with self._lock:
            previous = self.algorithm_tuning
            self.algorithm_tuning = tuning
            self._refresh_effective_thresholds()
            self._sim_cache_block = None
            should_force_cycle = self.control_mode == "automated"
        LOG.info("Action trigger (%s): set_algorithm_tuning %s -> %s", source, previous, tuning)
        self._persist_algorithm_tuning(tuning)
        result = {
            "algorithm_tuning": self.algorithm_tuning,
            "thresholds": deepcopy(self.cfg.thresholds.__dict__),
            "base_thresholds": deepcopy(self._base_thresholds),
        }
        if should_force_cycle:
            cycle_status = self.force_cycle(source=f"{source}_tuning_change")
            result["runtime"] = cycle_status
            result["cycle_started"] = True
        else:
            result["cycle_started"] = False
        return result

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
