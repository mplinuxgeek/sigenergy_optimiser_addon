from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from optimizer.config import AppConfig
from optimizer.fit_export_window_analysis import WindowCandidates, get_export_window_candidates
from optimizer.ha_client import EntityState, HAClient

LOG = logging.getLogger(__name__)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, "unknown", "unavailable", "none", ""):
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


def _state_float(states: dict[str, EntityState], entity_id: str, default: float = 0.0) -> float:
    s = states.get(entity_id)
    return _to_float(s.state if s else None, default)


def _is_on(states: dict[str, EntityState], entity_id: str) -> bool:
    s = states.get(entity_id)
    return bool(s and s.state == "on")


def _attr(states: dict[str, EntityState], entity_id: str, key: str, default: Any = None) -> Any:
    s = states.get(entity_id)
    if not s:
        return default
    return s.attributes.get(key, default)


def _bounded_number_value(states: dict[str, EntityState], entity_id: str, value: float) -> float:
    min_v = _to_float(_attr(states, entity_id, "min", None), float("-inf"))
    max_v = _to_float(_attr(states, entity_id, "max", None), float("inf"))
    bounded = value
    if bounded < min_v:
        bounded = min_v
    if bounded > max_v:
        bounded = max_v
    return bounded


def _parse_ts(value: Any, tz: ZoneInfo) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(tz)
    if not isinstance(value, str):
        return None
    text = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).astimezone(tz)
    except ValueError:
        return None


@dataclass
class Decision:
    reason: str
    desired_mode: str
    desired_export_limit: float
    desired_import_limit: float
    desired_pv_max_power_limit: float
    sunrise_soc_required: float
    battery_soc: float
    feedin_price: float
    current_price: float
    effective_ha_control: bool
    export_floor_soc: float = 0.0
    morning_dump_active: bool = False


class Optimizer:
    def __init__(self, config: AppConfig, client: HAClient, timezone: str = "Australia/Adelaide") -> None:
        self.cfg = config
        self.ha = client
        self.tz = ZoneInfo(timezone)
        self.last_daily_date: str | None = None
        self.last_morning_date: str | None = None
        self._export_hysteresis_on: bool = False

    def _forecast_pv_points_kw(self, states: dict[str, EntityState], entity_id: str) -> list[tuple[datetime, float]]:
        item = states.get(entity_id)
        if not item:
            return []
        attrs = item.attributes or {}
        rows: Any = attrs.get("detailedForecast") or attrs.get("detailedHourly") or attrs.get("forecast") or attrs.get("forecasts") or []
        if not isinstance(rows, list):
            return []

        out: list[tuple[datetime, float]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            ts = _parse_ts(row.get("period_start") or row.get("time") or row.get("start_time"), self.tz)
            if not ts:
                continue
            v = _to_float(
                row.get("pv_estimate")
                if row.get("pv_estimate") is not None
                else row.get("estimate")
                if row.get("estimate") is not None
                else row.get("value"),
                -1.0,
            )
            if v < 0:
                continue
            out.append((ts, max(0.0, v)))
        out.sort(key=lambda x: x[0])
        return out

    def _forecast_price_points(
        self,
        states: dict[str, EntityState],
        entity_ids: list[str],
    ) -> list[tuple[datetime, float]]:
        out: list[tuple[datetime, float]] = []
        seen: set[tuple[str, float]] = set()
        for entity_id in entity_ids:
            if not entity_id:
                continue
            item = states.get(entity_id)
            if not item:
                continue
            attrs = item.attributes or {}
            rows: Any = attrs.get("forecast") or attrs.get("forecasts") or []
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                ts = _parse_ts(row.get("time") or row.get("start_time") or row.get("period_start"), self.tz)
                if not ts:
                    continue
                price = _to_float(
                    row.get("value")
                    if row.get("value") is not None
                    else row.get("per_kwh")
                    if row.get("per_kwh") is not None
                    else row.get("estimate"),
                    float("nan"),
                )
                if price != price:
                    continue
                k = (ts.isoformat(), float(price))
                if k in seen:
                    continue
                seen.add(k)
                out.append((ts, float(price)))
        out.sort(key=lambda x: x[0])
        return out

    def _to_local_naive(self, ts: datetime) -> datetime:
        return ts.astimezone(self.tz).replace(tzinfo=None)

    def _to_local_aware(self, ts: Any) -> datetime | None:
        if ts is None:
            return None
        if hasattr(ts, "to_pydatetime"):
            ts = ts.to_pydatetime()
        if not isinstance(ts, datetime):
            return None
        if ts.tzinfo is None:
            return ts.replace(tzinfo=self.tz)
        return ts.astimezone(self.tz)

    def _build_price_table(
        self,
        states: dict[str, EntityState],
        *,
        now: datetime,
    ) -> list[dict[str, Any]]:
        e = self.cfg.entities

        def _series(entity_ids: list[str], current_entity_id: str) -> dict[datetime, float]:
            out: dict[datetime, float] = {}
            for ts, value in self._forecast_price_points(states, entity_ids):
                local_ts = self._to_local_naive(ts)
                if local_ts.date() != now.date():
                    continue
                out[local_ts] = float(value)
            current_item = states.get(current_entity_id)
            current_value = _to_float(current_item.state if current_item else None, float("nan"))
            if current_value == current_value:
                out[self._to_local_naive(now)] = float(current_value)
            return out

        fit_points = _series([e.feedin_forecast_sensor, e.feedin_sensor], e.feedin_sensor)
        if not fit_points:
            return []

        general_points = _series([e.price_forecast_sensor, e.price_sensor], e.price_sensor)
        all_times = sorted(set(fit_points) | set(general_points))
        fit_item = states.get(e.feedin_sensor) or states.get(e.feedin_forecast_sensor)
        spike_status = ""
        if fit_item:
            spike_status = str(fit_item.attributes.get("spike_status", "") or "").strip().lower()
        rows: list[dict[str, Any]] = []
        for ts in all_times:
            row: dict[str, Any] = {
                "time": ts,
                "fit": fit_points.get(ts, float("nan")),
            }
            general_value = general_points.get(ts)
            if general_value is not None:
                row["general"] = general_value
            if spike_status:
                row["spike_status"] = spike_status
            rows.append(row)
        return rows

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
        pv_kw = pv_w / 1000.0 if pv_w > 1000 else pv_w
        load_kw = load_w / 1000.0 if load_w > 1000 else load_w

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

        price_is_negative = price_is_actual and current_price < 0
        feedin_positive = feedin_price > 0
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

        # Solar-excess export: when the sun is up and FIT is positive, use the Solcast
        # near-term forecast to estimate how much surplus PV is available above load.
        # Charging the battery takes priority, so daytime solar surplus is only treated
        # as exportable once the battery has substantially recovered.
        solar_excess_min_battery_soc = 90.0
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
        # Fallback when detailedForecast is unavailable: estimate current power from
        # remaining forecast energy divided by hours remaining until sunset.  This gives
        # a conservative average-power estimate so solar-excess export isn't silently
        # disabled just because the Solcast attribute is missing or stale.
        if forecast_pv_now_kw == 0.0 and is_sun_up and hours_to_sunset > 0:
            forecast_pv_now_kw = forecast_remaining / max(0.5, hours_to_sunset)
        # Never estimate below actual measured output — if the inverter is already
        # producing more than the fallback average, use the real figure.
        forecast_pv_now_kw = max(forecast_pv_now_kw, pv_kw)
        forecast_excess_kw = max(0.0, forecast_pv_now_kw - load_kw)
        solar_excess_export_kw = (
            min(effective_export_cap_kw, forecast_excess_kw)
            if (
                feedin_positive
                and not price_is_negative
                and is_sun_up
                and battery_soc >= solar_excess_min_battery_soc
            )
            else 0.0
        )
        solar_excess_active = solar_excess_export_kw >= t.min_grid_transfer_kw
        LOG.debug(
            "_compute solar_excess: forecast_pv=%.2fkW pv=%.2fkW excess=%.2fkW "
            "solar_export=%.2fkW active=%s feedin_pos=%s sun_up=%s soc=%.1f%% soc_min=%.1f%%",
            forecast_pv_now_kw, pv_kw, forecast_excess_kw,
            solar_excess_export_kw, solar_excess_active, feedin_positive, is_sun_up,
            battery_soc, solar_excess_min_battery_soc,
        )

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
        elif not soc_above_export_floor and not morning_event_export_active and not post_morning_dump_hold_active and not solar_excess_active:
            desired_export = 0.0
            export_reason = f"Export blocked (reserve {export_floor_soc:.0f}%)"
        elif not export_hysteresis_allows and not morning_event_export_active and not post_morning_dump_hold_active and not solar_excess_active:
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
            if solar_excess_active:
                scaled_export = max(scaled_export, solar_excess_export_kw)
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
            elif solar_excess_active and export_tier == 0.0:
                export_reason = (
                    f"Solar excess {desired_export:.1f}kW "
                    f"(fcast {forecast_pv_now_kw:.1f}kW PV, {load_kw:.1f}kW load, {feedin_price*100:.0f}c FIT)"
                )
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
                    _solar_only = min(desired_export, solar_excess_export_kw)
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
            desired_export = min(desired_export, solar_excess_export_kw)
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
            desired_export = min(desired_export, solar_excess_export_kw)
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
                desired_export = min(desired_export, solar_excess_export_kw)
                if desired_export < t.min_grid_transfer_kw:
                    desired_export = 0.0
                export_reason = (
                    f"Battery export below WACS "
                    f"({feedin_price * 100:.0f}c < {_wacs * 100:.0f}c WACS)"
                )
                LOG.debug("_compute wacs_gate: feedin=%.4f wacs=%.4f", feedin_price, _wacs)

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
        if desired_mode == "Maximum Self Consumption" and desired_export == 0 and desired_import == 0 and (not is_sun_up):
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

    def _apply(self, states: dict[str, EntityState], d: Decision) -> None:
        e = self.cfg.entities
        t = self.cfg.thresholds
        actions_triggered: list[str] = []

        current_mode = states.get(e.ems_mode_select).state if states.get(e.ems_mode_select) else ""
        current_export = _state_float(states, e.grid_export_limit, 0)
        current_import = _state_float(states, e.grid_import_limit, 0)
        current_pv_cap = _state_float(states, e.pv_max_power_limit, t.pv_max_power_normal)

        LOG.debug(
            "_apply: current[mode=%r export=%.2f import=%.2f pv=%.2f] "
            "desired[mode=%r export=%.2f import=%.2f pv=%.2f] ha_ctrl=%s",
            current_mode, current_export, current_import, current_pv_cap,
            d.desired_mode, d.desired_export_limit, d.desired_import_limit,
            d.desired_pv_max_power_limit, d.effective_ha_control,
        )

        def _safe_call(label: str, fn) -> None:
            try:
                fn()
            except Exception as exc:
                LOG.warning("%s failed: %s", label, exc)

        _safe_call(
            "input_number.set_value min_soc_to_sunrise",
            lambda: self.ha.set_input_number(e.min_soc_to_sunrise_helper, d.sunrise_soc_required),
        )

        just_enabled_ha_control = False
        if t.auto_enable_ha_control and not _is_on(states, e.ha_control_switch) and d.effective_ha_control:
            _safe_call("switch.turn_on ha_control", lambda: self.ha.switch_on(e.ha_control_switch))
            actions_triggered.append(f"switch_on:{e.ha_control_switch}")
            just_enabled_ha_control = True

        if just_enabled_ha_control:
            LOG.info("HA control switch enabled; deferring mode/limit writes until next cycle")
            return

        if d.effective_ha_control and current_mode != d.desired_mode:
            actions_triggered.append(f"set_mode:{d.desired_mode}")
            _safe_call(
                "select.select_option ems_mode",
                lambda: self.ha.set_select(e.ems_mode_select, d.desired_mode),
            )
            # Force HA to poll the inverter so the entity state reflects the new
            # mode before we write the export/import limits.  Without this, HA
            # serves a stale cached state and _verify_and_reapply sees the old
            # mode on every read-back, triggering endless retries.
            _safe_call(
                f"homeassistant.update_entity {e.ems_mode_select}",
                lambda: self.ha.update_entity(e.ems_mode_select),
            )
            time.sleep(0.5)

        export_setpoint = _bounded_number_value(
            states,
            e.grid_export_limit,
            d.desired_export_limit if d.desired_export_limit > 0 else t.off_setpoint_kw,
        )
        import_setpoint = _bounded_number_value(
            states,
            e.grid_import_limit,
            d.desired_import_limit if d.desired_import_limit > 0 else t.off_setpoint_kw,
        )

        export_delta_to_target = abs(export_setpoint - current_export)
        import_delta_to_target = abs(import_setpoint - current_import)
        epsilon = 0.0005
        off_equiv_band = 0.15

        export_effectively_off = d.desired_export_limit == 0 and current_export <= (t.off_setpoint_kw + off_equiv_band)
        import_effectively_off = d.desired_import_limit == 0 and current_import <= (t.off_setpoint_kw + off_equiv_band)

        if d.effective_ha_control and (
            (not export_effectively_off)
            and export_delta_to_target > epsilon
            and (
                d.desired_export_limit == 0
                or export_delta_to_target >= t.min_change_threshold
                or current_export <= t.off_setpoint_kw
            )
        ):
            actions_triggered.append(f"set_export:{export_setpoint}")
            _safe_call(
                f"number.set_value grid_export_limit entity={e.grid_export_limit} value={export_setpoint}",
                lambda: self.ha.set_number(e.grid_export_limit, export_setpoint),
            )
            if d.desired_export_limit > 0:
                _safe_call("input_boolean.turn_on automated_export_flag", lambda: self.ha.bool_on(e.automated_export_flag))
            else:
                _safe_call("input_boolean.turn_off automated_export_flag", lambda: self.ha.bool_off(e.automated_export_flag))

        if d.effective_ha_control and (
            (not import_effectively_off)
            and import_delta_to_target > epsilon
            and (
                d.desired_import_limit == 0
                or import_delta_to_target >= t.min_change_threshold
                or current_import <= t.off_setpoint_kw
            )
        ):
            actions_triggered.append(f"set_import:{import_setpoint}")
            _safe_call(
                f"number.set_value grid_import_limit entity={e.grid_import_limit} value={import_setpoint}",
                lambda: self.ha.set_number(e.grid_import_limit, import_setpoint),
            )

        sun_state = states.get(e.sun_entity).state if states.get(e.sun_entity) else "unknown"
        pv_delta = abs(d.desired_pv_max_power_limit - current_pv_cap)
        allow_night_pv_cap = d.desired_pv_max_power_limit <= (t.off_setpoint_kw + 0.05)
        should_set_pv_cap = pv_delta >= 0.1 and (sun_state != "below_horizon" or allow_night_pv_cap)
        if should_set_pv_cap:
            pv_cap_setpoint = _bounded_number_value(states, e.pv_max_power_limit, d.desired_pv_max_power_limit)
            actions_triggered.append(f"set_pv_max:{pv_cap_setpoint}")
            _safe_call(
                f"number.set_value pv_max_power_limit entity={e.pv_max_power_limit} value={pv_cap_setpoint}",
                lambda: self.ha.set_number(e.pv_max_power_limit, pv_cap_setpoint),
            )

        # Flush the number entity caches so _verify_and_reapply_if_needed sees
        # the new values when it reads back 600 ms later.  Without this, HA
        # returns the last polled value from the integration's own scan interval.
        number_entities_written = [
            eid for lbl, eid in [
                ("set_export", e.grid_export_limit),
                ("set_import", e.grid_import_limit),
            ]
            if any(a.startswith(lbl) for a in actions_triggered)
        ]
        if number_entities_written:
            try:
                self.ha.update_entities(number_entities_written)
            except Exception as exc:
                LOG.warning("update_entities post-write failed: %s", exc)

        self._notify_import_export_transitions(states, d)
        self._notify_battery_events(states, d)

        current_reason = states.get(e.reason_text).state if states.get(e.reason_text) else ""
        if current_reason != d.reason:
            self.ha.logbook("SigEnergy Reason", d.reason)
            self.ha.set_input_text(e.reason_text, d.reason)

        if actions_triggered:
            LOG.info(
                "Action trigger (controller): %s | mode=%s export=%.2f import=%.2f pv_max=%.2f soc=%.2f reason=%s",
                ", ".join(actions_triggered),
                d.desired_mode,
                d.desired_export_limit,
                d.desired_import_limit,
                d.desired_pv_max_power_limit,
                d.battery_soc,
                d.reason,
            )
        else:
            LOG.debug(
                "_apply: no changes (mode=%r export=%.2f import=%.2f pv=%.2f soc=%.1f%%)",
                d.desired_mode, d.desired_export_limit, d.desired_import_limit,
                d.desired_pv_max_power_limit, d.battery_soc,
            )

    def _notify_import_export_transitions(self, states: dict[str, EntityState], d: Decision) -> None:
        svc = self.cfg.service.notification_service
        if not svc:
            return

        e = self.cfg.entities
        current_export = _state_float(states, e.grid_export_limit, 0)
        current_import = _state_float(states, e.grid_import_limit, 0)
        last_export = states.get(e.last_export_notification).state if states.get(e.last_export_notification) else ""
        last_import = states.get(e.last_import_notification).state if states.get(e.last_import_notification) else ""

        daily_export = _state_float(states, e.daily_export_energy, 0)
        daily_import = _state_float(states, e.daily_import_energy, 0)

        if current_export == 0 and d.desired_export_limit > 0 and last_export != "started":
            self.ha.set_input_number(e.export_session_start, daily_export)
            self.ha.notify(
                svc,
                "SigEnergy: Export Started",
                f"FIT ${d.feedin_price:.3f}/kWh, export {d.desired_export_limit:.2f}kW, SoC {d.battery_soc:.0f}%",
            )
            self.ha.set_input_text(e.last_export_notification, "started")

        if current_export > 0 and d.desired_export_limit == 0 and last_export != "stopped":
            start = _state_float(states, e.export_session_start, daily_export)
            session = max(0.0, daily_export - start)
            self.ha.notify(
                svc,
                "SigEnergy: Export Stopped",
                f"Session export {session:.3f}kWh, daily export {daily_export:.3f}kWh",
            )
            self.ha.set_input_text(e.last_export_notification, "stopped")

        if current_import == 0 and d.desired_import_limit > 0 and last_import != "started":
            self.ha.set_input_number(e.import_session_start, daily_import)
            self.ha.notify(
                svc,
                "SigEnergy: Import Started",
                f"Price ${d.current_price:.3f}/kWh, import {d.desired_import_limit:.2f}kW, SoC {d.battery_soc:.0f}%",
            )
            self.ha.set_input_text(e.last_import_notification, "started")

        if current_import > 0 and d.desired_import_limit == 0 and last_import != "stopped":
            start = _state_float(states, e.import_session_start, daily_import)
            session = max(0.0, daily_import - start)
            self.ha.notify(
                svc,
                "SigEnergy: Import Stopped",
                f"Session import {session:.3f}kWh, daily import {daily_import:.3f}kWh",
            )
            self.ha.set_input_text(e.last_import_notification, "stopped")

    def _notify_battery_events(self, states: dict[str, EntityState], d: Decision) -> None:
        svc = self.cfg.service.notification_service
        if not svc:
            return
        e = self.cfg.entities
        t = self.cfg.thresholds

        battery_soc = d.battery_soc
        armed = _is_on(states, e.battery_full_notification_armed)
        rearm_soc = min(t.battery_full_notification_rearm_soc, t.battery_full_notification_soc - 1)

        if battery_soc <= rearm_soc and not armed:
            self.ha.bool_on(e.battery_full_notification_armed)
            armed = True

        if battery_soc < t.sunrise_reserve_soc:
            self.ha.notify(
                svc,
                "Battery below reserve SoC",
                f"Battery {battery_soc:.0f}% below reserve {t.sunrise_reserve_soc:.0f}%",
            )

        if battery_soc <= 1:
            self.ha.notify(svc, "Battery Empty", f"Battery SoC {battery_soc:.0f}%")

        if armed and battery_soc >= t.battery_full_notification_soc:
            self.ha.notify(
                svc,
                "Battery Full",
                f"Battery SoC {battery_soc:.0f}% (re-arms at {rearm_soc:.0f}%)",
            )
            self.ha.bool_off(e.battery_full_notification_armed)

    def _send_summaries(self, states: dict[str, EntityState], d: Decision) -> None:
        s = self.cfg.service
        if not s.notification_service:
            return

        now = datetime.now(self.tz)
        now_day = now.strftime("%Y-%m-%d")
        now_hm = now.strftime("%H:%M")
        e = self.cfg.entities

        if s.notify_daily_summary and now_hm == s.daily_summary_time[:5] and self.last_daily_date != now_day:
            self.ha.notify(
                s.notification_service,
                "SigEnergy Summary",
                (
                    f"Use {_state_float(states, e.daily_load_energy, 0):.2f}kWh, "
                    f"PV {_state_float(states, e.daily_pv_energy, 0):.2f}kWh, "
                    f"Import {_state_float(states, e.daily_import_energy, 0):.2f}kWh, "
                    f"Export {_state_float(states, e.daily_export_energy, 0):.2f}kWh, "
                    f"SoC {d.battery_soc:.0f}%"
                ),
            )
            self.last_daily_date = now_day

        if s.notify_morning_summary and now_hm == s.morning_summary_time[:5] and self.last_morning_date != now_day:
            self.ha.notify(
                s.notification_service,
                "SigEnergy Morning",
                (
                    f"PV forecast today {_state_float(states, e.forecast_today_sensor, 0):.1f}kWh, "
                    f"battery discharge {_state_float(states, e.daily_battery_discharge_energy, 0):.2f}kWh, "
                    f"SoC {d.battery_soc:.0f}%"
                ),
            )
            self.last_morning_date = now_day
