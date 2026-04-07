# Part of OptimizerRuntime — see optimizer/runtime/__init__.py
from __future__ import annotations

import logging
from copy import deepcopy
from datetime import datetime
from typing import Any

from optimizer.config import AppConfig
from optimizer.controller import Optimizer
from optimizer.runtime._constants import CONTROL_MODES, ALGORITHM_TUNINGS

LOG = logging.getLogger(__name__)


class _SnapshotsMixin:
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
            "auto_profile_enabled": self.auto_profile_enabled,
            "auto_profile_summary": self._auto_profile_summary,
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
                "auto_profile_enabled": self.auto_profile_enabled,
                "auto_profile_summary": self._auto_profile_summary,
                "thresholds": {
                    "export_threshold_low": t.export_threshold_low,
                    "export_threshold_medium": t.export_threshold_medium,
                    "export_threshold_high": t.export_threshold_high,
                    "export_limit_low": t.export_limit_low,
                    "export_limit_medium": t.export_limit_medium,
                    "export_limit_high": t.export_limit_high,
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

    def reload_config_from_disk(self, *, source: str = "api") -> dict[str, Any]:
        """Reload config.yaml into the live runtime.

        HA connection changes are intentionally rejected for live reload because
        the current websocket/session lifecycle is bound to the existing client.
        Those edits are still valid on disk, but they require a process restart.
        """
        LOG.info("Action trigger (%s): reload_config_from_disk", source)
        new_cfg = AppConfig.load(self.config_path)
        new_cfg.validate()

        with self._lock:
            if new_cfg.ha_url != self.cfg.ha_url or new_cfg.ha_token != self.cfg.ha_token:
                raise ValueError(
                    "Config saved, but home_assistant.url/token changes require a service restart"
                )
            self.cfg = new_cfg
            self.poll_seconds = max(5, int(self.cfg.service.poll_seconds))
            self._config_midnight_reserve_floor = float(self.cfg.thresholds.midnight_reserve_soc)
            self._base_thresholds = deepcopy(self.cfg.thresholds.__dict__)
            self.optimizer = Optimizer(self.cfg, self.client, timezone=self.timezone)
            self._refresh_effective_thresholds()
            self._sim_cache_block = None
            self._sim_cache_result = None
            self._last_soc_int = None
            self.last_reload = self._now()
            self._last_reload_dt = datetime.now(self.tz)

        return {
            "status": self.status(),
            "config": self.public_config(),
        }
