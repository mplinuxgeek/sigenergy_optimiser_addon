# Part of Optimizer — see optimizer/controller/__init__.py
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

from optimizer.ha_client import EntityState
from optimizer.controller._helpers import (
    Decision,
    _to_float,
    _state_float,
    _is_on,
    _bounded_number_value,
)

LOG = logging.getLogger(__name__)


class _ApplyMixin:
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

        # Treat anything at or below the off-setpoint as "effectively off" so
        # "started"/"stopped" transitions fire correctly even when the off state
        # writes off_setpoint_kw (0.1) rather than 0.
        off_band = self.cfg.thresholds.off_setpoint_kw + 0.05

        if current_export <= off_band and d.desired_export_limit > 0 and last_export != "started":
            self.ha.set_input_number(e.export_session_start, daily_export)
            self.ha.notify(
                svc,
                "SigEnergy: Export Started",
                f"FIT ${d.feedin_price:.3f}/kWh, export {d.desired_export_limit:.2f}kW, SoC {d.battery_soc:.0f}%",
            )
            self.ha.set_input_text(e.last_export_notification, "started")

        if current_export > off_band and d.desired_export_limit == 0 and last_export != "stopped":
            start = _state_float(states, e.export_session_start, daily_export)
            session = max(0.0, daily_export - start)
            self.ha.notify(
                svc,
                "SigEnergy: Export Stopped",
                f"Session export {session:.3f}kWh, daily export {daily_export:.3f}kWh",
            )
            self.ha.set_input_text(e.last_export_notification, "stopped")

        if current_import <= off_band and d.desired_import_limit > 0 and last_import != "started":
            self.ha.set_input_number(e.import_session_start, daily_import)
            self.ha.notify(
                svc,
                "SigEnergy: Import Started",
                f"Price ${d.current_price:.3f}/kWh, import {d.desired_import_limit:.2f}kW, SoC {d.battery_soc:.0f}%",
            )
            self.ha.set_input_text(e.last_import_notification, "started")

        if current_import > off_band and d.desired_import_limit == 0 and last_import != "stopped":
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
            if not self._battery_low_notified:
                self.ha.notify(
                    svc,
                    "Battery below reserve SoC",
                    f"Battery {battery_soc:.0f}% below reserve {t.sunrise_reserve_soc:.0f}%",
                )
                self._battery_low_notified = True
        else:
            self._battery_low_notified = False

        if battery_soc <= 1:
            if not self._battery_empty_notified:
                self.ha.notify(svc, "Battery Empty", f"Battery SoC {battery_soc:.0f}%")
                self._battery_empty_notified = True
        else:
            self._battery_empty_notified = False

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
