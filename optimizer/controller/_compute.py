# Part of Optimizer — see optimizer/controller/__init__.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from optimizer.ha_client import EntityState
from optimizer.controller._helpers import (
    Decision,
    _to_float,
    _state_float,
    _is_on,
    _attr,
    _parse_ts,
)

LOG = logging.getLogger(__name__)


class _ComputeMixin:
    def _parse_weather_adverse(
        self,
        states: dict[str, EntityState],
        weather_entity: str,
        now: datetime,
    ) -> str:
        """Scan the HA weather entity's 24-hour forecast for adverse conditions.

        Returns 'storm' when lightning/thunderstorm/hail is predicted, 'wind' when
        sustained high winds are forecast, or '' when conditions are benign or the
        entity is not configured.
        """
        if not weather_entity:
            return ""
        item = states.get(weather_entity)
        if not item:
            return ""
        cutoff = now + timedelta(hours=24)
        worst = ""
        for entry in (item.attributes.get("forecast") or []):
            if not isinstance(entry, dict):
                continue
            ts = _parse_ts(entry.get("datetime") or entry.get("time"), self.tz)
            if ts and ts > cutoff:
                continue
            condition = str(entry.get("condition", "")).lower()
            if any(kw in condition for kw in ("lightning", "storm", "hail", "thunder")):
                return "storm"
            if any(kw in condition for kw in ("wind",)):
                worst = "wind"
            if _to_float(entry.get("wind_speed"), 0.0) >= 60.0:
                worst = "wind"
        return worst

    def _productive_solar_start(
        self,
        states: dict[str, EntityState],
        *,
        next_rising: datetime,
        load_kw: float,
    ) -> datetime:
        """Return the first forecast time after sunrise when PV output >= load.

        This is the "productive solar" moment — when the sun starts generating
        more power than the house consumes and the battery no longer needs to
        support load.  The morning dump window ends here so the battery arrives
        at the target SOC exactly as PV takes over.

        Falls back to next_rising + 2 h when forecast data is unavailable.
        """
        points = self._forecast_pv_points_kw(states, self.cfg.entities.forecast_today_sensor)
        threshold_kw = max(0.1, load_kw)
        for ts, pv in points:
            if ts < next_rising:
                continue
            if pv >= threshold_kw:
                return ts
        # Fallback: 2 h after sunrise keeps the window reasonable on cloudy days
        return next_rising + timedelta(hours=2)

    def run_cycle(self) -> Decision:
        states = self.ha.get_all_states()
        decision = self._compute(states)
        self._apply(states, decision)
        self._send_summaries(states, decision)
        return decision

    def _compute(self, states: dict[str, EntityState], now_dt: datetime | None = None) -> Decision:
        e = self.cfg.entities
        t = self.cfg.thresholds
        now = now_dt.astimezone(self.tz) if now_dt else datetime.now(self.tz)

        battery_soc = max(0.0, min(100.0, _state_float(states, e.battery_soc_sensor)))
        pv_w = _state_float(states, e.pv_power_sensor)
        load_w = _state_float(states, e.consumed_power_sensor)
        pv_uom = str(_attr(states, e.pv_power_sensor, "unit_of_measurement", "kW")).lower()
        load_uom = str(_attr(states, e.consumed_power_sensor, "unit_of_measurement", "kW")).lower()
        pv_kw = pv_w / 1000.0 if pv_uom == "w" else pv_w
        load_kw = load_w / 1000.0 if load_uom == "w" else load_w

        price_state = states.get(e.price_sensor)
        price_available = bool(price_state and price_state.state not in ("unknown", "unavailable", "none", ""))
        price_is_estimated = bool(price_state and price_state.attributes.get("estimate", False))
        price_is_actual = price_available and not price_is_estimated
        current_price = _to_float(price_state.state if price_state else None, 1.0)
        if not price_available:
            LOG.warning(
                "Price sensor '%s' unavailable (state=%r); defaulting to 1.0 $/kWh — imports blocked",
                e.price_sensor,
                price_state.state if price_state else "missing",
            )

        feedin_state = states.get(e.feedin_sensor)
        feedin_available = bool(feedin_state and feedin_state.state not in ("unknown", "unavailable", "none", ""))
        feedin_price = _to_float(feedin_state.state if feedin_state else None, -999.0)
        if not feedin_available:
            LOG.warning(
                "Feedin sensor '%s' unavailable (state=%r); exports blocked",
                e.feedin_sensor,
                feedin_state.state if feedin_state else "missing",
            )
        price_spike_active = _is_on(states, e.price_spike_sensor)
        negative_price_expected = _is_on(states, e.negative_price_expected_sensor) if e.negative_price_expected_sensor else False

        cap_raw = _state_float(states, e.rated_capacity_sensor, 20)
        cap_uom = str(_attr(states, e.rated_capacity_sensor, "unit_of_measurement", "kWh")).lower()
        battery_capacity_kwh = cap_raw / 1000.0 if cap_uom == "wh" else cap_raw
        battery_capacity_kwh = battery_capacity_kwh if battery_capacity_kwh > 0 else 20.0

        available_discharge_kwh = _state_float(states, e.available_discharge_sensor, 0)
        battery_fill_need_kwh = max(0.0, min(battery_capacity_kwh, battery_capacity_kwh - available_discharge_kwh))

        forecast_remaining = _state_float(states, e.forecast_remaining_sensor, 0)
        forecast_today = _state_float(states, e.forecast_today_sensor, 0)
        forecast_tomorrow = _state_float(states, e.forecast_tomorrow_sensor, 0)

        sun_state = states.get(e.sun_entity)
        next_rising = _parse_ts(sun_state.attributes.get("next_rising") if sun_state else None, self.tz)
        next_setting = _parse_ts(sun_state.attributes.get("next_setting") if sun_state else None, self.tz)
        is_sun_up = bool(sun_state and sun_state.state == "above_horizon")

        if next_rising:
            start = next_setting if is_sun_up and next_setting else now
            hours_to_sunrise = max(0.0, ((next_rising + timedelta(hours=1)) - start).total_seconds() / 3600)
        else:
            hours_to_sunrise = 6.0

        if next_setting:
            hours_to_sunset = max(0.0, (next_setting - now).total_seconds() / 3600)
        elif is_sun_up:
            hours_to_sunset = 12.0  # sun is up but next_setting unknown; avoid false evening trigger
        else:
            hours_to_sunset = 0.0

        is_evening_or_night = (hours_to_sunset <= 0.0) or (not is_sun_up and hours_to_sunrise < 18)
        # Midnight reserve is an evening protection: apply it only after local
        # sunset, not during the pre-dawn hours of the following morning.
        is_after_sunset = (
            not is_sun_up
            and next_rising is not None
            and next_rising.date() > now.date()
        )

        LOG.debug(
            "_compute inputs: soc=%.1f%% pv=%.2fkW load=%.2fkW price=%.4f feedin=%.4f "
            "price_ok=%s feedin_ok=%s spike=%s sun=%s hrs_sunrise=%.1f hrs_sunset=%.1f after_sunset=%s",
            battery_soc, pv_kw, load_kw, current_price, feedin_price,
            price_available, feedin_available, price_spike_active,
            "up" if is_sun_up else "down", hours_to_sunrise, hours_to_sunset, is_after_sunset,
        )

        hours_to_pv_cover_load = self._hours_until_pv_exceeds_load(
            states,
            now=now,
            load_kw=load_kw,
            fallback_hours=hours_to_sunrise,
        )
        reserve_kwh = battery_capacity_kwh * (t.sunrise_reserve_soc / 100.0)
        energy_needed_kwh = load_kw * hours_to_pv_cover_load * t.sunrise_safety_factor
        sunrise_required_soc = ((reserve_kwh + energy_needed_kwh) / battery_capacity_kwh) * 100.0
        sunrise_required_soc = max(0.0, min(100.0, sunrise_required_soc + t.sunrise_buffer_percent))
        sunrise_required_soc = max(sunrise_required_soc, t.sunrise_reserve_soc)

        daytime_floor_soc = max(0.0, min(100.0, t.min_soc_floor))
        nighttime_floor_soc = max(daytime_floor_soc, sunrise_required_soc)
        export_floor_soc = nighttime_floor_soc if is_evening_or_night else daytime_floor_soc

        # Forward-looking midnight reserve: raise the export floor so that the projected
        # SoC at midnight stays at or above midnight_reserve_soc.
        # net_charge_kwh = PV still to come today minus load between now and midnight.
        # If positive the battery will gain charge, so we can afford a lower floor now;
        # if negative we need more charge in reserve right now.
        # Initialise to the hard target so the relax block below always has a valid
        # forward-looking floor to clamp against, even when the inner condition is skipped.
        min_soc_now_for_midnight = t.midnight_reserve_soc
        if t.midnight_reserve_soc > 0 and is_after_sunset:
            midnight_local = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            hours_to_midnight = max(0.0, (midnight_local - now).total_seconds() / 3600.0)
            if 0 < hours_to_midnight < 20:
                # Use actual measured PV power as the signal that generation is
                # still ongoing — pv_kw is real inverter data and is zero as soon
                # as output stops.  The Solcast forecast_remaining sensor can stay
                # stale for hours after sunset, and the sun entity can report
                # 'above_horizon' in some HA configurations even post-sunset.
                # Requiring real PV output (>= min_grid_transfer_kw) means we stop
                # crediting forecast energy the moment generation actually ceases.
                effective_forecast_remaining = forecast_remaining if pv_kw >= t.min_grid_transfer_kw else 0.0
                net_charge_kwh = effective_forecast_remaining - load_kw * hours_to_midnight
                min_soc_now_for_midnight = t.midnight_reserve_soc - (net_charge_kwh / battery_capacity_kwh) * 100.0
                min_soc_now_for_midnight = max(0.0, min(100.0, min_soc_now_for_midnight))
                export_floor_soc = max(export_floor_soc, min_soc_now_for_midnight)
                LOG.debug(
                    "_compute midnight_reserve: hrs_to_midnight=%.2f eff_fc_remaining=%.2fkWh "
                    "net_charge=%.2fkWh min_soc_now=%.1f%% export_floor_before_relax=%.1f%%",
                    hours_to_midnight, effective_forecast_remaining,
                    net_charge_kwh, min_soc_now_for_midnight, export_floor_soc,
                )

        if battery_soc >= t.export_guard_relax_soc or price_spike_active or feedin_price >= t.export_threshold_high:
            export_floor_soc = max(daytime_floor_soc, export_floor_soc - t.sunrise_export_relax_percent)
            # Never relax the floor below the FORWARD-LOOKING midnight reserve floor
            # when the sun is down.  The relax is meant to allow deeper exports when
            # the battery is well above the sunrise requirement; it must not override
            # the energy budget that keeps us at or above midnight_reserve_soc by 00:00.
            # Use min_soc_now_for_midnight (the forward-looking floor) rather than the
            # fixed midnight_reserve_soc target — clamping to the target alone allows
            # the relax to consume the overnight load margin and leave midnight SoC
            # short of the target.
            if t.midnight_reserve_soc > 0 and is_after_sunset and pv_kw < t.min_grid_transfer_kw:
                export_floor_soc = max(export_floor_soc, min_soc_now_for_midnight)

        # --- Balanced: variable time-of-day floor ---
        # Rises linearly from a low morning floor to a higher afternoon floor so the
        # battery is progressively reserved for the expensive evening period.
        if t.variable_floor_enabled and is_sun_up:
            hour_frac = now.hour + now.minute / 60.0
            t_frac = (hour_frac - t.variable_floor_morning_hour) / max(
                0.01, t.variable_floor_afternoon_hour - t.variable_floor_morning_hour
            )
            t_frac = max(0.0, min(1.0, t_frac))
            vf_floor = t.variable_floor_morning_soc + t_frac * (
                t.variable_floor_afternoon_soc - t.variable_floor_morning_soc
            )
            if vf_floor > export_floor_soc:
                export_floor_soc = vf_floor
                LOG.debug(
                    "_compute variable_floor: hour=%.1f vf_floor=%.1f%% export_floor=%.1f%%",
                    hour_frac, vf_floor, export_floor_soc,
                )

        # --- Balanced: evening gap reserve ---
        # Reserve enough battery capacity to cover home load during the evening gap
        # window (default 6 PM–10 PM), with a 20% safety margin.
        if t.evening_gap_reserve_enabled:
            gap_hours = max(0.0, t.evening_gap_end_hour - t.evening_gap_start_hour)
            gap_kwh = gap_hours * load_kw * 1.20
            gap_floor_soc = min(80.0, (gap_kwh / max(0.1, battery_capacity_kwh)) * 100.0)
            if gap_floor_soc > export_floor_soc:
                export_floor_soc = gap_floor_soc
                LOG.debug(
                    "_compute evening_gap: gap_kwh=%.2f gap_floor=%.1f%% export_floor=%.1f%%",
                    gap_kwh, gap_floor_soc, export_floor_soc,
                )

        # --- Max Consumption: dynamic weather-based reserve ---
        # If adverse weather (storm or high winds) is forecast in the next 24 h,
        # raise the export floor to the configured storm/wind reserve level so the
        # battery is full for a potential outage or prolonged generation gap.
        if t.dynamic_reserve_enabled:
            _weather_adverse = self._parse_weather_adverse(states, e.weather_entity, now)
            if _weather_adverse == "storm" and t.dynamic_reserve_storm_soc > export_floor_soc:
                export_floor_soc = t.dynamic_reserve_storm_soc
                LOG.info(
                    "_compute weather_reserve: storm predicted, floor raised to %.1f%%",
                    t.dynamic_reserve_storm_soc,
                )
            elif _weather_adverse == "wind" and t.dynamic_reserve_wind_soc > export_floor_soc:
                export_floor_soc = t.dynamic_reserve_wind_soc
                LOG.info(
                    "_compute weather_reserve: high winds predicted, floor raised to %.1f%%",
                    t.dynamic_reserve_wind_soc,
                )

        LOG.debug(
            "_compute floors: daytime=%.1f%% nighttime=%.1f%% export_floor=%.1f%% "
            "sunrise_req=%.1f%% h_to_pv=%.1fh evening=%s",
            daytime_floor_soc, nighttime_floor_soc, export_floor_soc,
            sunrise_required_soc, hours_to_pv_cover_load, is_evening_or_night,
        )

        ess_max_discharge = _state_float(states, e.ess_rated_discharge_power_sensor, 999)
        if ess_max_discharge > 1000:
            ess_max_discharge /= 1000.0
        if ess_max_discharge <= 0:
            ess_max_discharge = 999.0

        ess_max_charge = _state_float(states, e.ess_rated_charge_power_sensor, 999)
        if ess_max_charge > 1000:
            ess_max_charge /= 1000.0
        if ess_max_charge <= 0:
            ess_max_charge = 999.0

        price_is_negative = price_is_actual and current_price < -0.01  # ignore sub-1c rounding artefacts
        feedin_positive = feedin_price >= 0.01                           # ignore sub-1c rounding artefacts
        fit_start = t.export_threshold_low + max(0.0, t.fit_hysteresis_band)
        fit_stop = max(0.0, t.export_threshold_low - max(0.0, t.fit_hysteresis_band))
        _prev_hysteresis = self._export_hysteresis_on
        if feedin_price >= fit_start:
            self._export_hysteresis_on = True
        elif feedin_price <= fit_stop:
            self._export_hysteresis_on = False
        if self._export_hysteresis_on != _prev_hysteresis:
            LOG.debug(
                "_compute export_hysteresis: %s → %s (feedin=%.4f fit_start=%.4f fit_stop=%.4f)",
                "ON" if _prev_hysteresis else "OFF",
                "ON" if self._export_hysteresis_on else "OFF",
                feedin_price, fit_start, fit_stop,
            )

        effective_export_cap_kw = min(25.0, ess_max_discharge, max(0.0, t.export_limit_high))

        if price_spike_active or feedin_price >= t.export_threshold_high:
            export_tier = effective_export_cap_kw
        elif feedin_price >= t.export_threshold_medium:
            frac = (feedin_price - t.export_threshold_medium) / max(
                0.001, (t.export_threshold_high - t.export_threshold_medium)
            )
            export_tier = t.export_limit_medium + frac * (effective_export_cap_kw - t.export_limit_medium)
        elif feedin_price >= t.export_threshold_low:
            frac = (feedin_price - t.export_threshold_low) / max(
                0.001, (t.export_threshold_medium - t.export_threshold_low)
            )
            export_tier = t.export_limit_low + frac * (t.export_limit_medium - t.export_limit_low)
        else:
            export_tier = 0.0
        export_tier = min(effective_export_cap_kw, max(0.0, export_tier))

        can_export_price = feedin_positive and not price_is_negative
        export_hysteresis_allows = self._export_hysteresis_on or feedin_price >= t.export_threshold_medium or price_spike_active
        # Keep anti-flap hysteresis for normal operation, but allow deeper
        # discharge closer to the configured floor when FIT is at least medium.
        floor_guard = t.soc_hysteresis_percent
        if feedin_price >= t.export_threshold_medium:
            floor_guard = min(floor_guard, 0.25)
        soc_above_export_floor = battery_soc >= (export_floor_soc + floor_guard)
        # Morning dump intentionally allows discharge down to the configured minimum SoC floor.
        # This window is already delayed close to sunrise to preserve overnight capacity first.
        # Morning dump targets the configured SOC floor (morning_dump_target_soc, default 2.5%)
        # rather than the general daytime_floor_soc.  Productive solar will refill the battery
        # from that low point without any battery discharge required.
        morning_dump_floor_soc = t.morning_dump_target_soc
        soc_above_morning_dump_floor = battery_soc > morning_dump_floor_soc
        morning_dump_target_kw = min(effective_export_cap_kw, max(t.min_grid_transfer_kw, effective_export_cap_kw * t.morning_dump_rate_fraction))
        energy_above_floor_kwh = max(0.0, (battery_soc - morning_dump_floor_soc) * 0.01 * battery_capacity_kwh)
        # Size the dump window using the export-only rate.  House load drains the battery
        # faster than this, so we reach the target slightly *before* the window end —
        # the safe, correct direction.  Using (export + load) would underestimate the window
        # because PV starts offsetting load after sunrise, slowing the drain near the end.
        target_dump_duration_h = max(0.5, energy_above_floor_kwh / max(0.1, morning_dump_target_kw))
        # Anchor the window end to when PV is forecast to first exceed load ("productive solar").
        # The dump intentionally continues past sunrise until that moment, so the is_sun_up
        # guard is replaced by the dump_end boundary.
        productive_solar_dt = (
            self._productive_solar_start(states, next_rising=next_rising, load_kw=load_kw)
            if next_rising
            else None
        )
        dump_start, dump_end = self._morning_dump_window(
            states,
            now=now,
            next_rising=next_rising,
            preferred_hours=max(0.5, t.morning_dump_hours_before_sunrise),
            target_duration_h=target_dump_duration_h,
            productive_solar_dt=productive_solar_dt,
        )
        # Max Consumption: auto-enable morning dump when a high-generation day is
        # forecast so the battery has headroom to absorb peak solar production.
        _morning_dump_auto = (
            t.morning_space_creation_enabled
            and battery_soc >= 90.0
            and forecast_remaining >= t.morning_space_forecast_kwh
        )
        morning_dump_active = (
            (t.morning_dump_enabled or _morning_dump_auto)
            and feedin_price >= t.morning_dump_min_feedin
            and dump_start is not None
            and dump_end is not None
            and now >= dump_start
            and now <= dump_end
            and soc_above_morning_dump_floor
        )
        LOG.debug(
            "_compute morning_dump: active=%s enabled=%s sun_up=%s soc_above_floor=%s "
            "feedin=%.4f min_feedin=%.4f dump=%s\u2013%s productive_solar=%s",
            morning_dump_active, t.morning_dump_enabled, is_sun_up, soc_above_morning_dump_floor,
            feedin_price, t.morning_dump_min_feedin,
            dump_start.strftime("%H:%M") if dump_start else "None",
            dump_end.strftime("%H:%M") if dump_end else "None",
            productive_solar_dt.strftime("%H:%M") if productive_solar_dt else "None",
        )
        # Max-profits continuation: after the scheduled dump window closes,
        # keep the battery-discharge event alive through the live morning peak
        # instead of immediately re-arming the midnight reserve and lookahead
        # hold. This is intentionally narrow to the market-arbitrage profile.
        morning_dump_continuation_active = False
        best_morning_candidate_start: datetime | None = None
        if (
            t.morning_dump_enabled
            and t.forced_export_on_spike_enabled
            and t.forecast_hold_enabled
            and not morning_dump_active
            and dump_end is not None
            and now > dump_end
            and now.hour < t.late_morning_hour
            and battery_soc > morning_dump_floor_soc
            and feedin_positive
            and not price_is_negative
        ):
            _morning_candidates = self._window_candidates(
                states,
                now=now,
                period="morning",
                top_n=10,
                morning_end_hour=max(1, min(23, t.late_morning_hour)),
                window_minutes=60,
                exclude_spike=True,
            )
            for _, row in _morning_candidates.iterrows():
                start_dt = self._to_local_aware(row.get("start_time"))
                end_dt = self._to_local_aware(row.get("window_end_time"))
                if start_dt is None or end_dt is None or dump_end is None:
                    continue
                if end_dt <= dump_end:
                    continue
                best_morning_candidate_start = start_dt
                morning_dump_continuation_active = now < end_dt
                break
        morning_event_export_active = morning_dump_active or morning_dump_continuation_active
        post_morning_dump_hold_active = (
            t.morning_dump_enabled
            and not morning_event_export_active
            and dump_end is not None
            and now > dump_end
            and not price_is_negative
            and battery_soc > export_floor_soc
        )

        # forecast_pv_now_kw: used by the midnight-reserve hard-stop check below.
        _pv_pts = self._forecast_pv_points_kw(states, e.forecast_today_sensor)
        forecast_pv_now_kw = 0.0
        if _pv_pts and is_sun_up:
            _window_s = 45 * 60  # ±45 min window centred on now
            _nearby = [(ts, kw) for ts, kw in _pv_pts if abs((ts - now).total_seconds()) <= _window_s]
            if _nearby:
                forecast_pv_now_kw = min(_nearby, key=lambda x: abs((x[0] - now).total_seconds()))[1]
            else:
                _upcoming = [(ts, kw) for ts, kw in _pv_pts if ts >= now]
                if _upcoming:
                    forecast_pv_now_kw = _upcoming[0][1]
        if forecast_pv_now_kw == 0.0 and is_sun_up and hours_to_sunset > 0:
            forecast_pv_now_kw = forecast_remaining / max(0.5, hours_to_sunset)
        forecast_pv_now_kw = max(forecast_pv_now_kw, pv_kw)

        if not can_export_price:
            desired_export = 0.0
            export_reason = "Export blocked (loss)"
        elif (
            t.midnight_reserve_soc > 0
            and is_after_sunset
            and battery_soc <= t.midnight_reserve_soc
            and (not is_sun_up or forecast_pv_now_kw < t.ess_first_discharge_pv_threshold_kw)
            and not morning_event_export_active
        ):
            # Hard stop: SoC at/below midnight reserve with no meaningful PV output.
            # Condition uses BOTH sun entity and forecast_pv_now_kw so that a
            # mis-reported sun entity (HA timezone/location edge cases) cannot
            # override the protection.  forecast_pv_now_kw reflects real Solcast
            # data and measured pv_kw, so is reliably 0 after sunset.
            # Exception: morning_dump_active intentionally discharges below
            # midnight_reserve_soc toward the 2.5% target — the pre-sunrise export
            # window is the correct time to drain that reserve, and productive solar
            # will immediately refill it.
            desired_export = 0.0
            export_reason = f"Export blocked (at/below midnight reserve {t.midnight_reserve_soc:.0f}%)"
            LOG.debug(
                "_compute: midnight reserve hard-stop "
                "(soc=%.1f%% <= midnight_reserve=%.1f%% sun_up=%s fc_pv=%.2fkW)",
                battery_soc, t.midnight_reserve_soc, is_sun_up, forecast_pv_now_kw,
            )
        elif not soc_above_export_floor and not morning_event_export_active and not post_morning_dump_hold_active:
            desired_export = 0.0
            export_reason = f"Export blocked (reserve {export_floor_soc:.0f}%)"
        elif not export_hysteresis_allows and not morning_event_export_active and not post_morning_dump_hold_active:
            desired_export = 0.0
            export_reason = f"Export blocked (FIT hysteresis {feedin_price*100:.0f}c)"
        else:
            soc_ramp = (battery_soc - export_floor_soc) / max(1.0, 100.0 - export_floor_soc)
            soc_ramp = max(0.0, min(1.0, soc_ramp))
            scaled_export = export_tier * soc_ramp
            if morning_event_export_active:
                # Sustain export for the full dump window by tapering when remaining
                # energy above floor cannot support the nominal target rate.
                remaining_dump_h = max(1.0 / 60.0, (dump_end - now).total_seconds() / 3600.0) if dump_end else (1.0 / 60.0)
                sustainable_kw = energy_above_floor_kwh / remaining_dump_h
                tapered_dump_kw = min(morning_dump_target_kw, max(0.0, sustainable_kw))
                scaled_export = max(scaled_export, tapered_dump_kw)
            if post_morning_dump_hold_active:
                hold_export_kw = min(
                    effective_export_cap_kw,
                    max(t.export_limit_medium, effective_export_cap_kw * t.morning_dump_rate_fraction),
                )
                scaled_export = max(scaled_export, hold_export_kw)
            desired_export = min(effective_export_cap_kw, max(0.0, scaled_export))
            if desired_export < t.min_grid_transfer_kw:
                desired_export = 0.0
            if morning_dump_active:
                export_reason = f"Morning dump export {desired_export:.1f}kW to floor {morning_dump_floor_soc:.1f}%"
            elif morning_dump_continuation_active:
                _target_label = (
                    best_morning_candidate_start.strftime("%H:%M")
                    if best_morning_candidate_start is not None
                    else "best window"
                )
                export_reason = (
                    f"Morning peak continuation {desired_export:.1f}kW "
                    f"toward {_target_label} to floor {morning_dump_floor_soc:.1f}%"
                )
            elif post_morning_dump_hold_active:
                export_reason = f"Post-morning-dump hold {desired_export:.1f}kW until FIT turns negative"
            else:
                export_reason = f"Export {feedin_price * 100:.0f}c"

        # Floor protection applies to export only:
        # once at/below floor, only allow export from real excess solar.
        # When morning dump is active, honour the lower morning_dump_floor_soc so
        # the dump can drain through the general daytime_floor_soc down to 2.5%.
        excess_solar_kw = max(0.0, pv_kw - load_kw)
        active_floor_soc = morning_dump_floor_soc if morning_event_export_active else daytime_floor_soc
        if battery_soc <= (active_floor_soc + 0.05):
            desired_export = min(desired_export, excess_solar_kw)
            export_reason = f"Export limited to excess solar at floor ({desired_export:.1f}kW)"
            if desired_export < t.min_grid_transfer_kw:
                desired_export = 0.0

        # Look-ahead: if a significantly higher export price is forecast within
        # the look-ahead window, hold back battery-backed exports now to preserve
        # capacity for that more valuable window.  Solar surplus (excess PV over
        # load) is still allowed since curtailing it would waste free generation.
        if (
            desired_export > 0
            and not morning_event_export_active
            and feedin_price > 0
        ):
            _daytime_candidates = self._window_candidates(
                states,
                now=now,
                period="daytime",
                top_n=10,
                daytime_start_hour=9,
                daytime_end_hour=15,
                window_minutes=60,
                exclude_spike=True,
            )
            _future_target = 0.0
            _future_start: datetime | None = None
            for _, row in _daytime_candidates.iterrows():
                start_dt = self._to_local_aware(row.get("start_time"))
                if start_dt is None or start_dt <= now:
                    continue
                _future_start = start_dt
                _future_target = max(
                    float(row.get("avg_fit_next_hour", 0.0) or 0.0),
                    float(row.get("fit_at_start", 0.0) or 0.0),
                )
                break
            if _future_target > 0 and _future_start is not None:
                _ratio_hold = (
                    t.afternoon_lookahead_hours > 0
                    and t.afternoon_lookahead_ratio > 1.0
                    and _future_target > feedin_price * t.afternoon_lookahead_ratio
                )
                _fraction_hold = (
                    t.afternoon_lookahead_min_fraction > 0
                    and feedin_price < _future_target * t.afternoon_lookahead_min_fraction
                )
                if _ratio_hold or _fraction_hold:
                    # Cap to solar surplus only — don't draw down the battery
                    _solar_only = min(desired_export, excess_solar_kw)
                    if _solar_only < t.min_grid_transfer_kw:
                        _solar_only = 0.0
                    if _solar_only < desired_export - 0.01:
                        LOG.debug(
                            "_compute lookahead: holding battery export via daytime candidate "
                            "(feedin=%.4f target=%.4f start=%s ratio=%.1fx threshold=%.1fx "
                            "min_fraction=%.3f ratio_hold=%s fraction_hold=%s) export %.1f→%.1fkW",
                            feedin_price, _future_target,
                            _future_start.strftime("%H:%M"),
                            _future_target / feedin_price if feedin_price > 0 else 0,
                            t.afternoon_lookahead_ratio,
                            t.afternoon_lookahead_min_fraction,
                            _ratio_hold, _fraction_hold,
                            desired_export, _solar_only,
                        )
                        desired_export = _solar_only
                        export_reason = (
                            f"Export held for {_future_target * 100:.0f}c daytime window "
                            f"at {_future_start.strftime('%H:%M')} (now {feedin_price * 100:.0f}c)"
                        )

        # ---------------------------------------------------------------
        # Profile-specific export overrides (applied after lookahead)
        # ---------------------------------------------------------------

        # --- Max Profits: pre-compute forecast hold state ---
        # Determines whether a high-value export event is approaching so the battery
        # can be held at maximum charge until that event window opens.
        _forecast_hold_active = False
        _forecast_hold_max_price = 0.0
        if (
            t.forecast_hold_enabled
            and t.forecast_hold_start_hour <= now.hour < t.forecast_hold_end_hour
            and not morning_event_export_active
        ):
            _evening_candidates = self._window_candidates(
                states,
                now=now,
                period="evening",
                top_n=5,
                evening_start_hour=19,
                window_minutes=60,
                exclude_spike=True,
            )
            for _, row in _evening_candidates.iterrows():
                start_dt = self._to_local_aware(row.get("start_time"))
                if start_dt is None or start_dt <= now:
                    continue
                _forecast_hold_max_price = max(
                    float(row.get("avg_fit_next_hour", 0.0) or 0.0),
                    float(row.get("fit_at_start", 0.0) or 0.0),
                )
                if _forecast_hold_max_price >= t.forecast_hold_price_threshold:
                    _forecast_hold_active = True
                    LOG.debug(
                        "_compute forecast_hold: active=True start=%s target=%.4f",
                        start_dt.strftime("%H:%M"), _forecast_hold_max_price,
                    )
                break

        # --- Max Profits: forced max export on price spike ---
        # When the feed-in tariff hits the spike threshold, export at full inverter
        # capacity regardless of the SoC floor — the logic is that buying energy back
        # later at standard rates is cheaper than the foregone spike revenue.
        if (
            t.forced_export_on_spike_enabled
            and feedin_available
            and feedin_price >= t.forced_export_spike_threshold
            and battery_soc > daytime_floor_soc + 1.0
            and not _forecast_hold_active
        ):
            desired_export = effective_export_cap_kw
            export_reason = (
                f"FORCED spike export {feedin_price * 100:.0f}c "
                f"(threshold {t.forced_export_spike_threshold * 100:.0f}c)"
            )
            LOG.info(
                "_compute forced_spike_export: feedin=%.4f threshold=%.4f export=%.2fkW",
                feedin_price, t.forced_export_spike_threshold, desired_export,
            )

        # --- Max Profits: hold battery for upcoming high-price event ---
        # Cap export to solar excess only so the battery reaches the event at 100%.
        elif _forecast_hold_active and desired_export > 0:
            desired_export = min(desired_export, excess_solar_kw)
            if desired_export < t.min_grid_transfer_kw:
                desired_export = 0.0
            export_reason = f"Holding for {_forecast_hold_max_price * 100:.0f}c forecast event"
            LOG.debug("_compute forecast_hold: capped export to solar-only=%.2fkW", desired_export)

        # --- Max Consumption: battery saturation export gate ---
        # Solar is routed home → battery → grid.  Grid export from battery is only
        # permitted once the battery is effectively full (default 98 %).
        # Solar-excess export (PV > load, battery full) is always allowed.
        if (
            t.battery_saturation_export_enabled
            and desired_export > 0
            and battery_soc < t.battery_saturation_export_soc
            and not morning_event_export_active
            and not (t.forced_export_on_spike_enabled and feedin_price >= t.forced_export_spike_threshold)
        ):
            desired_export = min(desired_export, excess_solar_kw)
            if desired_export < t.min_grid_transfer_kw:
                desired_export = 0.0
            if desired_export == 0.0:
                export_reason = (
                    f"Export reserved until battery full "
                    f"({battery_soc:.0f}% < {t.battery_saturation_export_soc:.0f}%)"
                )
            LOG.debug(
                "_compute battery_saturation: soc=%.1f%% threshold=%.1f%%",
                battery_soc, t.battery_saturation_export_soc,
            )

        # --- Balanced: WACS export gate ---
        # Only discharge the battery to the grid when the feed-in tariff exceeds the
        # Weighted Average Cost of Storage: (buy_price / efficiency) + degradation.
        # Solar-excess export and spike events bypass this gate.
        if (
            t.wacs_export_gate_enabled
            and desired_export > 0
            and not morning_event_export_active
            and not price_spike_active
            and not (t.forced_export_on_spike_enabled and feedin_price >= t.forced_export_spike_threshold)
        ):
            _wacs = t.wacs_buy_price / max(0.01, t.wacs_round_trip_efficiency) + t.wacs_degradation_cost_per_kwh
            if feedin_price < _wacs:
                desired_export = min(desired_export, excess_solar_kw)
                if desired_export < t.min_grid_transfer_kw:
                    desired_export = 0.0
                export_reason = (
                    f"Battery export below WACS "
                    f"({feedin_price * 100:.0f}c < {_wacs * 100:.0f}c WACS)"
                )
                LOG.debug("_compute wacs_gate: feedin=%.4f wacs=%.4f", feedin_price, _wacs)

        # --- Low solar export protection ---
        # Block battery-backed export when the Solcast forecast is insufficient to
        # refill the battery.  Rule: forecast < fill_need × factor → export blocked.
        # While the sun is up, uses the remaining-today forecast; after sunset,
        # uses tomorrow's forecast.  Solar-excess export (PV > load) is always
        # allowed since that route does not discharge the battery.
        # Spike exports also bypass this gate — rare high-value events are worth the
        # short-term cost of buying back at standard rates.
        if (
            t.low_solar_export_protection_enabled
            and desired_export > 0
            and not morning_event_export_active
            and not (t.forced_export_on_spike_enabled and feedin_price >= t.forced_export_spike_threshold)
        ):
            _relevant_forecast = forecast_remaining if is_sun_up else forecast_tomorrow
            _low_solar_threshold_kwh = battery_fill_need_kwh * t.low_solar_export_factor
            if _relevant_forecast < _low_solar_threshold_kwh:
                desired_export = min(desired_export, excess_solar_kw)
                if desired_export < t.min_grid_transfer_kw:
                    desired_export = 0.0
                _period_label = "remaining today" if is_sun_up else "tomorrow"
                export_reason = (
                    f"Export blocked (low solar: {_relevant_forecast:.1f}kWh {_period_label} "
                    f"< {_low_solar_threshold_kwh:.1f}kWh needed to fill)"
                )
                LOG.debug(
                    "_compute low_solar_protection: forecast=%.2fkWh threshold=%.2fkWh "
                    "fill_need=%.2fkWh factor=%.1f",
                    _relevant_forecast, _low_solar_threshold_kwh,
                    battery_fill_need_kwh, t.low_solar_export_factor,
                )

        # ---------------------------------------------------------------
        # Grid import decision
        # ---------------------------------------------------------------
        reserve_deficit_soc = max(0.0, sunrise_required_soc - battery_soc)
        if price_is_negative:
            # Any negative price: charge as hard as allowed by inverter/caps.
            desired_import = min(t.import_limit_high, t.cap_total_import, ess_max_charge)
            import_reason = f"Importing max ({current_price*100:.0f}c)"
        elif current_price > 0:
            desired_import = 0.0
            import_reason = f"Import blocked (loss {current_price*100:.0f}c)"
        elif feedin_price >= t.export_threshold_low and battery_soc >= sunrise_required_soc:
            desired_import = 0.0
            import_reason = "Import blocked (export condition)"
        elif reserve_deficit_soc > t.soc_hysteresis_percent:
            desired_import = 0.0
            import_reason = f"Import blocked (reserve deficit {reserve_deficit_soc:.1f}%, non-negative price)"
        else:
            desired_import = 0.0
            import_reason = "Import blocked (non-negative price)"

        if desired_import < t.min_grid_transfer_kw:
            desired_import = 0.0

        # --- Max Profits: pre-charge for upcoming high-price event ---
        # When an event hold is active and the battery is not full, trickle-charge
        # from the grid at cheap rates so the battery arrives at the event window full.
        if (
            _forecast_hold_active
            and battery_soc < 99.0
            and desired_import == 0.0
            and not price_is_negative
            and current_price <= t.cheap_import_price_threshold * 3.0
        ):
            desired_import = min(t.import_limit_medium, ess_max_charge)
            import_reason = f"Pre-charging for {_forecast_hold_max_price * 100:.0f}c event"
            LOG.debug("_compute pre_charge: import=%.2fkW", desired_import)

        # --- Max Consumption: conditional grid import gate ---
        # Import is only justified when the battery is low AND tomorrow's solar
        # forecast is insufficient to recover to the safety floor unaided.
        if t.conditional_grid_import_enabled and not price_is_negative:
            if desired_import == 0.0 and (
                current_price <= t.cheap_import_price_threshold
                and battery_soc < t.min_soc_floor + 10.0
                and forecast_tomorrow < t.conditional_grid_import_solar_kwh
            ):
                # Allow import at cheap rates when battery is low and solar won't recover
                desired_import = t.import_limit_low
                import_reason = (
                    f"Safety import ({battery_soc:.0f}% low, "
                    f"tomorrow {forecast_tomorrow:.1f}kWh)"
                )
                LOG.debug(
                    "_compute conditional_import: triggered soc=%.1f%% tomorrow=%.2fkWh",
                    battery_soc, forecast_tomorrow,
                )
            elif desired_import > 0 and (
                forecast_tomorrow >= t.conditional_grid_import_solar_kwh
                and battery_soc >= t.min_soc_floor
            ):
                # Block non-critical import when tomorrow's solar will cover the shortfall
                desired_import = 0.0
                import_reason = f"Import waived (tomorrow {forecast_tomorrow:.1f}kWh forecast)"
                LOG.debug(
                    "_compute conditional_import: waived tomorrow=%.2fkWh",
                    forecast_tomorrow,
                )
            if desired_import < t.min_grid_transfer_kw:
                desired_import = 0.0

        if desired_export > 0:
            # Post-morning-dump export should stay in PV-first as requested, even
            # when forecast PV is low. Otherwise retain the ESS-first fallback.
            if not post_morning_dump_hold_active and forecast_pv_now_kw < t.ess_first_discharge_pv_threshold_kw:
                desired_mode = t.ess_first_mode_option
            else:
                desired_mode = t.export_mode_option
        elif desired_import > 0:
            desired_mode = "Command Charging (Grid First)" if price_is_negative else "Command Charging (PV First)"
        else:
            desired_mode = "Maximum Self Consumption"

        current_mode = states.get(e.ems_mode_select).state if states.get(e.ems_mode_select) else ""
        ha_control_enabled = _is_on(states, e.ha_control_switch)
        needs_ha_control_switch = (
            t.auto_enable_ha_control
            and not ha_control_enabled
            and (desired_export > 0 or desired_import > 0 or current_mode != desired_mode)
        )

        desired_pv_cap = t.pv_max_power_normal
        # Apply a night-idle PV cap only when exports are genuinely market-blocked
        # (price below threshold) — not when a profile like no_exports has
        # artificially raised thresholds to 99c to prevent any grid export.
        # In the latter case PV must stay at max so solar always serves the home.
        _exports_policy_blocked = t.export_threshold_low >= 1.0
        if (
            not _exports_policy_blocked
            and desired_mode == "Maximum Self Consumption"
            and desired_export == 0
            and desired_import == 0
            and (not is_sun_up)
        ):
            desired_pv_cap = max(0.1, min(t.pv_max_power_normal, t.off_setpoint_kw))

        reason = f"{export_reason}; {import_reason}; cover {hours_to_pv_cover_load:.1f}h"

        LOG.debug(
            "_compute decision: mode=%s export=%.2f import=%.2f floor=%.1f%% soc=%.1f%% | %s",
            desired_mode, desired_export, desired_import, export_floor_soc, battery_soc, reason,
        )

        return Decision(
            reason=reason[:95],
            desired_mode=desired_mode,
            desired_export_limit=round(desired_export, 2),
            desired_import_limit=round(desired_import, 2),
            desired_pv_max_power_limit=round(desired_pv_cap, 2),
            sunrise_soc_required=round(sunrise_required_soc, 2),
            battery_soc=round(battery_soc, 2),
            feedin_price=round(feedin_price, 4),
            current_price=round(current_price, 4),
            effective_ha_control=ha_control_enabled or needs_ha_control_switch,
            export_floor_soc=round(export_floor_soc, 2),
            morning_dump_active=morning_dump_active,
        )
