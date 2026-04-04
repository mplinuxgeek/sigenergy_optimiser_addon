"""Export window candidate ranking for FIT prices without heavy numeric deps."""

from __future__ import annotations

import math
import warnings
from datetime import datetime, timedelta
from statistics import stdev
from typing import Any, Iterable


class WindowCandidates(list):
    @property
    def empty(self) -> bool:
        return len(self) == 0

    def iterrows(self):
        for idx, row in enumerate(self):
            yield idx, row


def _is_finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(float(value))


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value in (None, "", 0, "0", "false", "False", "FALSE", "off", "OFF"):
        return False
    return True


def _normalise_rows(price_table: Any) -> list[dict[str, Any]]:
    if isinstance(price_table, list):
        rows = [dict(row) for row in price_table if isinstance(row, dict)]
    else:
        raise TypeError("price_table must be a list of row dicts indexed by 'time'")

    out: list[dict[str, Any]] = []
    for row in rows:
        ts = row.get("time")
        if not isinstance(ts, datetime):
            continue
        fit = row.get("fit")
        if not _is_finite_number(fit):
            continue
        item = {"time": ts, "fit": float(fit)}
        for key in ("general", "renewables", "predicted_low", "predicted_high"):
            value = row.get(key)
            if _is_finite_number(value):
                item[key] = float(value)
        if "is_estimate" in row:
            item["is_estimate"] = _coerce_bool(row.get("is_estimate"))
        spike_status = row.get("spike_status")
        if spike_status is not None:
            item["spike_status"] = str(spike_status).strip().lower()
        out.append(item)
    out.sort(key=lambda row: row["time"])
    return out


def get_export_window_candidates(
    price_table,
    period: str,
    top_n: int = 10,
    morning_end_hour: int = 9,
    evening_start_hour: int = 19,
    daytime_start_hour: int = 9,
    daytime_end_hour: int = 15,
    window_minutes: int = 60,
    renewables_weight: float = 0.0,
    exclude_spike: bool = False,
    min_samples: int = 1,
):
    if top_n < 1:
        raise ValueError("top_n must be >= 1")
    if window_minutes < 1:
        raise ValueError("window_minutes must be a positive integer")
    if min_samples < 1:
        raise ValueError("min_samples must be >= 1")
    if period not in ("morning", "daytime", "evening"):
        raise ValueError("period must be 'morning', 'daytime', or 'evening'")
    if not _is_finite_number(renewables_weight):
        raise ValueError("renewables_weight must be a finite number")
    for name, value in (
        ("morning_end_hour", morning_end_hour),
        ("evening_start_hour", evening_start_hour),
        ("daytime_start_hour", daytime_start_hour),
        ("daytime_end_hour", daytime_end_hour),
    ):
        if not isinstance(value, int) or not (0 <= value <= 23):
            raise ValueError(f"{name} must be in [0, 23], got {value}")
    if morning_end_hour < 1:
        raise ValueError("morning_end_hour must be >= 1")
    if daytime_start_hour >= daytime_end_hour:
        raise ValueError(
            f"daytime_start_hour ({daytime_start_hour}) must be less than daytime_end_hour ({daytime_end_hour})"
        )

    rows = _normalise_rows(price_table)
    if not rows:
        return WindowCandidates()

    window_delta = timedelta(minutes=window_minutes)
    candidate_rows: list[dict[str, Any]] = []
    spike_data_seen = False

    for idx, row in enumerate(rows):
        ts = row["time"]
        hour = ts.hour
        if period == "morning":
            if hour >= morning_end_hour:
                continue
            period_end = ts.replace(hour=morning_end_hour, minute=0, second=0, microsecond=0)
        elif period == "evening":
            if hour < evening_start_hour:
                continue
            period_end = (ts.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
        else:
            if hour < daytime_start_hour or hour >= daytime_end_hour:
                continue
            period_end = ts.replace(hour=daytime_end_hour, minute=0, second=0, microsecond=0)

        if exclude_spike:
            spike_status = str(row.get("spike_status", "")).strip().lower()
            if spike_status:
                spike_data_seen = True
            if spike_status in ("spike", "potential"):
                continue

        window_end = ts + window_delta
        if window_end > period_end:
            continue

        window_rows: list[dict[str, Any]] = []
        j = idx
        while j < len(rows) and rows[j]["time"] < window_end:
            window_rows.append(rows[j])
            j += 1
        if len(window_rows) < min_samples:
            continue

        durations_sec: list[float] = []
        for pos, win_row in enumerate(window_rows):
            current_ts = win_row["time"]
            next_ts = window_rows[pos + 1]["time"] if pos + 1 < len(window_rows) else window_end
            durations_sec.append(max(0.0, (next_ts - current_ts).total_seconds()))
        total_window_sec = max(1.0, window_delta.total_seconds())

        fit_values = [float(win_row["fit"]) for win_row in window_rows]
        weighted_avg_fit = sum(v * d for v, d in zip(fit_values, durations_sec)) / total_window_sec

        result_row: dict[str, Any] = {
            "start_time": ts,
            "window_end_time": window_end,
            "fit_at_start": float(row["fit"]),
            "avg_fit_next_hour": weighted_avg_fit,
            "min_fit_next_hour": min(fit_values),
            "max_fit_next_hour": max(fit_values),
            "fit_std_next_hour": float(stdev(fit_values)) if len(fit_values) > 1 else float("nan"),
            "samples_in_window": len(window_rows),
        }

        general_value = row.get("general")
        if _is_finite_number(general_value):
            result_row["general_at_start"] = float(general_value)
            result_row["fit_vs_general"] = float(row["fit"]) - float(general_value)

        renewables_values = [float(win_row["renewables"]) for win_row in window_rows if _is_finite_number(win_row.get("renewables"))]
        if renewables_values:
            result_row["avg_renewables"] = sum(renewables_values) / len(renewables_values)

        low_values = [win_row.get("predicted_low") for win_row in window_rows]
        high_values = [win_row.get("predicted_high") for win_row in window_rows]
        if len(low_values) == len(window_rows) and all(_is_finite_number(v) for v in low_values):
            result_row["conf_low"] = sum(float(v) * d for v, d in zip(low_values, durations_sec)) / total_window_sec
        if len(high_values) == len(window_rows) and all(_is_finite_number(v) for v in high_values):
            result_row["conf_high"] = sum(float(v) * d for v, d in zip(high_values, durations_sec)) / total_window_sec

        if any(_coerce_bool(win_row.get("is_estimate", False)) for win_row in window_rows):
            result_row["has_estimate"] = True

        result_row["_composite"] = weighted_avg_fit + float(renewables_weight) * (result_row.get("avg_renewables", 0.0) / 100.0)
        candidate_rows.append(result_row)

    if exclude_spike and not spike_data_seen:
        warnings.warn(
            "exclude_spike=True but no spike_status data aligned with fit timestamps; spike filter was not applied.",
            UserWarning,
            stacklevel=2,
        )

    if not candidate_rows:
        warnings.warn(
            f"No valid {period} export windows found (window_minutes={window_minutes}, min_samples={min_samples}). "
            "Try reducing min_samples or window_minutes.",
            UserWarning,
            stacklevel=2,
        )
        return WindowCandidates()

    candidate_rows.sort(
        key=lambda row: (
            -float(row["_composite"]),
            -float(row["fit_at_start"]),
            row["start_time"],
        )
    )
    trimmed = WindowCandidates(candidate_rows[:top_n])
    for row in trimmed:
        row.pop("_composite", None)
        for key, places in (
            ("fit_at_start", 4),
            ("avg_fit_next_hour", 4),
            ("min_fit_next_hour", 4),
            ("max_fit_next_hour", 4),
            ("fit_std_next_hour", 4),
            ("general_at_start", 4),
            ("fit_vs_general", 4),
            ("conf_low", 4),
            ("conf_high", 4),
            ("avg_renewables", 2),
        ):
            value = row.get(key)
            if _is_finite_number(value):
                row[key] = round(float(value), places)
    return trimmed
