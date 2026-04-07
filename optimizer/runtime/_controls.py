# Part of OptimizerRuntime — see optimizer/runtime/__init__.py
from __future__ import annotations

import logging
import time
from typing import Any

from optimizer.runtime._constants import CONTROL_MODES

LOG = logging.getLogger(__name__)


class _ControlsMixin:
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
