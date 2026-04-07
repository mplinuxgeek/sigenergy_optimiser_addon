# Part of OptimizerRuntime — see optimizer/runtime/__init__.py
from __future__ import annotations

import logging
from bisect import bisect_right
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any

from optimizer.controller import Decision, Optimizer
from optimizer.ha_client import EntityState
from optimizer.runtime._constants import ALGORITHM_TUNINGS

LOG = logging.getLogger(__name__)


class _SimulationMixin:
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
