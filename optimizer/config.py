from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class Entities:
    manual_mode_select: str = ""
    optimiser_automation: str = ""
    ha_control_switch: str = "switch.sigen_plant_remote_ems_controled_by_home_assistant"
    ems_mode_select: str = "select.sigen_plant_remote_ems_control_mode"
    grid_export_limit: str = "number.sigen_plant_grid_export_limitation"
    grid_import_limit: str = "number.sigen_plant_grid_import_limitation"
    pv_max_power_limit: str = "number.sigen_plant_pv_max_power_limit"
    ess_max_charging_limit: str = ""
    ess_max_discharging_limit: str = ""
    battery_soc_sensor: str = "sensor.sigen_plant_battery_state_of_charge"
    battery_power_sensor: str = "sensor.sigen_plant_battery_power"
    pv_power_sensor: str = "sensor.sigen_plant_pv_power"
    consumed_power_sensor: str = "sensor.sigen_plant_consumed_power"
    grid_import_power_sensor: str = "sensor.sigen_plant_grid_import_power"
    grid_export_power_sensor: str = "sensor.sigen_plant_grid_export_power"
    rated_capacity_sensor: str = "sensor.sigen_plant_rated_energy_capacity"
    available_discharge_sensor: str = "sensor.sigen_plant_available_max_discharging_capacity"
    ess_rated_discharge_power_sensor: str = "sensor.sigen_inverter_ess_rated_discharge_power"
    ess_rated_charge_power_sensor: str = "sensor.sigen_inverter_ess_rated_charge_power"
    daily_export_energy: str = "sensor.sigen_plant_daily_grid_export_energy"
    daily_import_energy: str = "sensor.sigen_plant_daily_grid_import_energy"
    daily_load_energy: str = "sensor.sigen_plant_daily_load_consumption"
    daily_battery_charge_energy: str = "sensor.sigen_plant_daily_battery_charge_energy"
    daily_battery_discharge_energy: str = "sensor.sigen_plant_daily_battery_discharge_energy"
    daily_pv_energy: str = "sensor.sigen_plant_daily_pv_energy"
    forecast_remaining_sensor: str = "sensor.solcast_pv_forecast_forecast_remaining_today"
    forecast_today_sensor: str = "sensor.solcast_pv_forecast_forecast_today"
    forecast_tomorrow_sensor: str = "sensor.solcast_pv_forecast_forecast_tomorrow"
    price_sensor: str = "sensor.amber_general_price"
    feedin_sensor: str = "sensor.amber_feed_in_price"
    price_forecast_sensor: str = ""
    feedin_forecast_sensor: str = ""
    demand_window_sensor: str = "binary_sensor.amber_demand_window"
    price_spike_sensor: str = "binary_sensor.amber_price_spike"
    negative_price_expected_sensor: str = ""
    weather_entity: str = ""
    sun_entity: str = "sun.sun"
    automated_export_flag: str = "input_boolean.sigenergy_automated_export"
    battery_full_notification_armed: str = "input_boolean.sigenergy_battery_full_notification_armed"
    export_session_start: str = "input_number.sigenergy_export_session_start_kwh"
    import_session_start: str = "input_number.sigenergy_import_session_start_kwh"
    min_soc_to_sunrise_helper: str = "input_number.battery_min_soc_to_last_till_sunrise"
    reason_text: str = "input_text.sigenergy_reason"
    last_export_notification: str = "input_text.sigenergy_last_export_notification"
    last_import_notification: str = "input_text.sigenergy_last_import_notification"


@dataclass
class Thresholds:
    export_threshold_low: float = 0.10
    export_threshold_medium: float = 0.20
    export_threshold_high: float = 1.00
    export_limit_low: float = 3.0
    export_limit_medium: float = 8.0
    export_limit_high: float = 25.0
    import_threshold_low: float = 0.0
    import_threshold_medium: float = -0.15
    import_threshold_high: float = -0.30
    import_limit_low: float = 5.0
    import_limit_medium: float = 15.0
    import_limit_high: float = 25.0
    max_price_threshold: float = 0.10
    cap_total_import: float = 25.0
    pv_max_power_normal: float = 25.0
    min_grid_transfer_kw: float = 0.5
    min_change_threshold: float = 0.2
    min_soc_floor: float = 15.0
    sunrise_reserve_soc: float = 20.0
    sunrise_buffer_percent: float = 5.0
    sunrise_safety_factor: float = 1.2
    night_reserve_soc: float = 15.0
    night_reserve_buffer: float = 5.0
    export_guard_relax_soc: float = 90.0
    forecast_safety_charging: float = 1.2
    forecast_safety_export: float = 1.2
    daytime_topup_max_soc: float = 90.0
    target_battery_charge: float = 2.0
    battery_full_notification_soc: float = 99.0
    battery_full_notification_rearm_soc: float = 90.0
    allow_low_medium_export_positive_fit: bool = True
    allow_positive_fit_battery_discharging: bool = False
    auto_enable_ha_control: bool = True
    off_setpoint_kw: float = 0.1
    ess_limit_value: float = 25.0
    ess_limits_require_command_modes: bool = True
    force_mode_use_rated_limits: bool = True
    fit_hysteresis_band: float = 0.01
    soc_hysteresis_percent: float = 2.0
    sunrise_export_relax_percent: float = 5.0
    cheap_import_price_threshold: float = 0.03
    reserve_protection_max_price: float = 0.08
    midnight_reserve_soc: float = 70.0
    enable_charge_holdoff: bool = True
    late_morning_hour: int = 11
    morning_dump_enabled: bool = False
    morning_dump_hours_before_sunrise: float = 1.5
    morning_dump_min_feedin: float = 0.10
    morning_dump_target_soc: float = 2.5
    afternoon_lookahead_hours: float = 6.0
    afternoon_lookahead_ratio: float = 3.0
    afternoon_lookahead_min_fraction: float = 0.95
    export_limit_value: float = 25.0
    import_limit_value: float = 25.0
    pv_max_power_value: float = 25.0
    morning_dump_rate_fraction: float = 0.75
    export_mode_option: str = "Command Discharging (PV First)"
    ess_first_mode_option: str = "Command Discharging (ESS First)"
    ess_first_discharge_pv_threshold_kw: float = 1.0
    import_mode_option: str = "Command Charging (Grid First)"
    block_mode_option: str = "Maximum Self Consumption"
    automated_option: str = "Automated"
    manual_option: str = "Manual"
    full_export_option: str = "Force Full Export"
    full_import_option: str = "Force Full Import"
    block_flow_option: str = "Prevent Import & Export"

    # --- Max Consumption: off-grid / energy-independence behaviours ---
    # Raise the export floor dynamically when adverse weather is forecast.
    dynamic_reserve_enabled: bool = False
    dynamic_reserve_storm_soc: float = 50.0   # Floor when lightning/storm predicted (%)
    dynamic_reserve_wind_soc: float = 35.0    # Floor when high winds predicted (%)
    # Battery must be this full before any battery-backed export is allowed.
    # Solar-excess export (PV > load) bypasses this gate and is always permitted.
    battery_saturation_export_enabled: bool = False
    battery_saturation_export_soc: float = 98.0
    # Auto-enable morning dump when a high-generation day is forecast (creates
    # space in a full battery for the incoming solar).
    morning_space_creation_enabled: bool = False
    morning_space_forecast_kwh: float = 15.0
    # Only import from the grid when battery is low AND tomorrow's solar is
    # insufficient to recover to the safety floor by sunset.
    conditional_grid_import_enabled: bool = False
    conditional_grid_import_solar_kwh: float = 5.0

    # --- Max Profits: market-arbitrage behaviours ---
    # Force maximum discharge to grid when the feed-in tariff hits or exceeds a spike
    # threshold, even if the battery SoC floor would normally block export.
    forced_export_on_spike_enabled: bool = False
    forced_export_spike_threshold: float = 1.00  # $/kWh
    # If a high-value export event is detected in the upcoming forecast window, hold the
    # battery at maximum charge until that event arrives, then fire the full discharge.
    forecast_hold_enabled: bool = False
    forecast_hold_price_threshold: float = 0.50   # Minimum upcoming FIT to trigger hold ($/kWh)
    forecast_hold_start_hour: int = 14             # Earliest hour to activate a hold
    forecast_hold_end_hour: int = 22               # Latest hour hold can remain active

    # --- Balanced: smart-hybrid / opportunity-cost behaviours ---
    # Only export stored battery energy when the feed-in tariff exceeds the Weighted
    # Average Cost of Storage (buy_price / efficiency + degradation).
    wacs_export_gate_enabled: bool = False
    wacs_buy_price: float = 0.30               # Average grid import cost ($/kWh)
    wacs_round_trip_efficiency: float = 0.90   # Battery round-trip efficiency (0–1)
    wacs_degradation_cost_per_kwh: float = 0.02  # Estimated degradation cost per kWh cycled
    # Reserve enough battery capacity to cover home load during the evening gap window
    # (default 6 PM – 10 PM).  Exports that would drain below this reserve are blocked.
    evening_gap_reserve_enabled: bool = False
    evening_gap_start_hour: int = 18
    evening_gap_end_hour: int = 22
    # Floating SOC floor that rises linearly from morning to afternoon so the battery
    # is progressively protected for the expensive evening period.
    variable_floor_enabled: bool = False
    variable_floor_morning_soc: float = 10.0    # Floor at or before morning_hour (%)
    variable_floor_afternoon_soc: float = 40.0  # Floor at or after afternoon_hour (%)
    variable_floor_morning_hour: int = 9
    variable_floor_afternoon_hour: int = 16


@dataclass
class ServiceConfig:
    poll_seconds: int = 20
    notification_service: str = ""
    notify_daily_summary: bool = True
    daily_summary_time: str = "23:55"
    notify_morning_summary: bool = True
    morning_summary_time: str = "07:30"
    sigenergy_config_entry_id: str = ""
    enable_daily_autotune: bool = True
    autotune_run_hour: int = 0
    autotune_target_net: float = 2.0
    autotune_min_soc_hard_floor: float = 4.0
    autotune_min_sunrise_soc: float = 8.0
    autotune_max_candidates: int = 80


@dataclass
class AppConfig:
    ha_url: str
    ha_token: str
    entities: Entities = field(default_factory=Entities)
    thresholds: Thresholds = field(default_factory=Thresholds)
    service: ServiceConfig = field(default_factory=ServiceConfig)
    # Per-profile threshold overrides from the config.yaml [profiles:] section.
    # Applied after profile math runs, so these values win over hardcoded profile defaults.
    profile_overrides: dict[str, dict[str, Any]] = field(default_factory=dict)

    @staticmethod
    def load(path: str) -> "AppConfig":
        raw = yaml.safe_load(Path(path).read_text()) or {}
        entities = Entities(**(raw.get("entities") or {}))
        thresholds = Thresholds(**(raw.get("thresholds") or {}))
        service = ServiceConfig(**(raw.get("service") or {}))
        ha = raw.get("home_assistant") or {}
        profile_overrides = {
            k: dict(v)
            for k, v in (raw.get("profiles") or {}).items()
            if isinstance(v, dict)
        }
        return AppConfig(
            ha_url=str(ha.get("url", "")).rstrip("/"),
            ha_token=str(ha.get("token", "")),
            entities=entities,
            thresholds=thresholds,
            service=service,
            profile_overrides=profile_overrides,
        )

    def validate(self) -> None:
        if not self.ha_url:
            raise ValueError("home_assistant.url is required")
        if not self.ha_token:
            raise ValueError("home_assistant.token is required")
        if not self.ha_url.startswith("http"):
            raise ValueError("home_assistant.url must start with http/https")
        t = self.thresholds
        # SoC bounds
        for name, val in [
            ("min_soc_floor", t.min_soc_floor),
            ("sunrise_reserve_soc", t.sunrise_reserve_soc),
            ("midnight_reserve_soc", t.midnight_reserve_soc),
            ("night_reserve_soc", t.night_reserve_soc),
            ("export_guard_relax_soc", t.export_guard_relax_soc),
            ("daytime_topup_max_soc", t.daytime_topup_max_soc),
            ("battery_saturation_export_soc", t.battery_saturation_export_soc),
            ("dynamic_reserve_storm_soc", t.dynamic_reserve_storm_soc),
            ("dynamic_reserve_wind_soc", t.dynamic_reserve_wind_soc),
            ("variable_floor_morning_soc", t.variable_floor_morning_soc),
            ("variable_floor_afternoon_soc", t.variable_floor_afternoon_soc),
        ]:
            if not (0.0 <= val <= 100.0):
                raise ValueError(f"thresholds.{name} must be between 0 and 100, got {val}")
        # Export threshold ordering
        if not (t.export_threshold_low <= t.export_threshold_medium <= t.export_threshold_high):
            raise ValueError(
                f"export thresholds must be non-decreasing: "
                f"low={t.export_threshold_low} medium={t.export_threshold_medium} high={t.export_threshold_high}"
            )
        # Import threshold ordering (lower is more negative i.e. cheaper)
        if not (t.import_threshold_medium <= t.import_threshold_low):
            raise ValueError(
                f"import_threshold_medium ({t.import_threshold_medium}) must be <= import_threshold_low ({t.import_threshold_low})"
            )
        # kW limits must be non-negative
        for name, val in [
            ("export_limit_low", t.export_limit_low),
            ("export_limit_medium", t.export_limit_medium),
            ("export_limit_high", t.export_limit_high),
            ("import_limit_low", t.import_limit_low),
            ("import_limit_medium", t.import_limit_medium),
            ("import_limit_high", t.import_limit_high),
            ("min_grid_transfer_kw", t.min_grid_transfer_kw),
            ("ess_first_discharge_pv_threshold_kw", t.ess_first_discharge_pv_threshold_kw),
        ]:
            if val < 0.0:
                raise ValueError(f"thresholds.{name} must be >= 0, got {val}")
