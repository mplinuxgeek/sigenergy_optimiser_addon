# Part of OptimizerRuntime — see optimizer/runtime/__init__.py
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Callable

LOG = logging.getLogger(__name__)


class _HaBridgeMixin:
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
