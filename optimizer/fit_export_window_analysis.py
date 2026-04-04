"""fit_export_window_analysis.py — export window candidate ranking for FIT prices."""

import math
import warnings

import numpy as np
import pandas as pd

# Preferred output column order (columns absent from a given run are skipped)
_COLUMN_ORDER = [
    "start_time",
    "window_end_time",
    "fit_at_start",
    "avg_fit_next_hour",
    "min_fit_next_hour",
    "max_fit_next_hour",
    "fit_std_next_hour",
    "samples_in_window",
    "general_at_start",
    "fit_vs_general",
    "avg_renewables",
    "conf_low",
    "conf_high",
    "has_estimate",
]


def get_export_window_candidates(
    price_table: pd.DataFrame,
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
) -> pd.DataFrame:
    """Rank candidate export start times within a given period.

    For each valid start time in the period the function looks at the following
    ``window_minutes`` and calculates a time-weighted average FIT price.
    Candidates are ranked by a composite score (highest first), then by FIT at
    start (highest first), then by start time (earliest first).

    Parameters
    ----------
    price_table:
        DataFrame indexed by naive local timestamps with at least a ``fit``
        column.  Optional enrichment columns: ``general``, ``renewables``,
        ``predicted_low``, ``predicted_high``, ``is_estimate``, ``spike_status``.
    period:
        One of ``'morning'``, ``'daytime'``, or ``'evening'``.
    top_n:
        Maximum number of candidates to return.  Must be >= 1.
    morning_end_hour:
        Hour (exclusive) at which the morning period ends.  Must be in [1, 23].
        Default 9.
    evening_start_hour:
        Hour (inclusive) at which the evening period starts.  Default 19.
    daytime_start_hour:
        Hour (inclusive) at which the daytime period starts.  Default 9.
    daytime_end_hour:
        Hour (exclusive) at which the daytime period ends.  Default 15.
    window_minutes:
        Length of the evaluation window in minutes.  Must be >= 1.  Default 60.
    renewables_weight:
        Weight applied to the average renewables percentage when computing the
        composite score.  Units are FIT-price per 1 % renewables — e.g. with FIT
        in $/kWh, a value of ``0.001`` adds at most ~0.1 $/kWh for 100 %
        renewables.  Must be a finite number.  Set to 0.0 (default) to rank by
        price only.
    exclude_spike:
        When True, candidates whose window start coincides with a ``spike`` or
        ``potential`` spike status are excluded.  Rows where ``spike_status`` is
        NaN are treated as non-spike (not excluded).  A warning is emitted if
        ``exclude_spike=True`` but no spike data aligns with the fit timestamps.
    min_samples:
        Minimum number of FIT samples required within the window for a candidate
        to be included.  Default 1 (no filtering).  Raise to 2 or higher to
        drop candidates backed by a single data point on sparse days.

    Returns
    -------
    pd.DataFrame
        Columns always present:

        - ``start_time``
        - ``window_end_time`` — timestamp at which the evaluation window closes
        - ``fit_at_start``
        - ``avg_fit_next_hour``
        - ``min_fit_next_hour`` — lowest FIT price within the window
        - ``max_fit_next_hour``
        - ``fit_std_next_hour`` — standard deviation of FIT prices within the
          window (NaN for single-sample windows)
        - ``samples_in_window``

        Columns present when the corresponding enrichment data is available:

        - ``general_at_start`` — general tariff price at the window start
        - ``fit_vs_general`` — ``fit_at_start - general_at_start``
        - ``avg_renewables`` — mean renewables percentage over the window
        - ``conf_low`` — time-weighted mean predicted low over the window
        - ``conf_high`` — time-weighted mean predicted high over the window
        - ``has_estimate`` — True if any interval in the window is a forecast
    """
    # --- Input validation ---
    if not isinstance(price_table, pd.DataFrame):
        raise TypeError("price_table must be a pandas DataFrame")
    if "fit" not in price_table.columns:
        raise ValueError("price_table must contain a 'fit' column")
    if window_minutes <= 0:
        raise ValueError("window_minutes must be a positive integer")
    if top_n < 1:
        raise ValueError("top_n must be >= 1")
    if min_samples < 1:
        raise ValueError("min_samples must be >= 1")
    if not isinstance(renewables_weight, (int, float)) or not math.isfinite(
        renewables_weight
    ):
        raise ValueError("renewables_weight must be a finite number")
    for _name, _val in [
        ("morning_end_hour", morning_end_hour),
        ("evening_start_hour", evening_start_hour),
        ("daytime_start_hour", daytime_start_hour),
        ("daytime_end_hour", daytime_end_hour),
    ]:
        if not (0 <= _val <= 23):
            raise ValueError(f"{_name} must be in [0, 23], got {_val}")
    if morning_end_hour < 1:
        raise ValueError("morning_end_hour must be >= 1")
    if daytime_start_hour >= daytime_end_hour:
        raise ValueError(
            f"daytime_start_hour ({daytime_start_hour}) must be less than "
            f"daytime_end_hour ({daytime_end_hour})"
        )
    if period not in ("morning", "daytime", "evening"):
        raise ValueError("period must be 'morning', 'daytime', or 'evening'")

    # --- Build fit series ---
    fit_series = (
        pd.to_numeric(price_table["fit"], errors="coerce")
        .dropna()
        .sort_index()
    )
    if fit_series.empty:
        return pd.DataFrame()

    fit_series.index = pd.DatetimeIndex(fit_series.index)
    fit_index = fit_series.index

    # --- Period mask ---
    if period == "morning":
        allowed_mask = fit_index.hour < morning_end_hour
    elif period == "evening":
        allowed_mask = fit_index.hour >= evening_start_hour
    else:  # daytime
        allowed_mask = (fit_index.hour >= daytime_start_hour) & (
            fit_index.hour < daytime_end_hour
        )

    # --- Consolidate optional enrichment series into a single dict ---
    enrichment: dict[str, pd.Series] = {}
    if "general" in price_table.columns:
        enrichment["general"] = (
            pd.to_numeric(price_table["general"], errors="coerce")
            .reindex(fit_series.index, method="ffill")
        )
    if "renewables" in price_table.columns:
        enrichment["renewables"] = (
            pd.to_numeric(price_table["renewables"], errors="coerce")
            .reindex(fit_series.index, method="ffill")
        )
    if (
        "predicted_low" in price_table.columns
        and "predicted_high" in price_table.columns
    ):
        enrichment["predicted_low"] = (
            pd.to_numeric(price_table["predicted_low"], errors="coerce")
            .reindex(fit_series.index, method="ffill")
        )
        enrichment["predicted_high"] = (
            pd.to_numeric(price_table["predicted_high"], errors="coerce")
            .reindex(fit_series.index, method="ffill")
        )
    if "is_estimate" in price_table.columns:
        enrichment["is_estimate"] = (
            price_table["is_estimate"]
            .reindex(fit_series.index, method="ffill")
            .fillna(False)
            .astype(bool)
        )

    # --- Apply spike exclusion mask ---
    if "spike_status" in price_table.columns and exclude_spike:
        spike_series = (
            price_table["spike_status"].reindex(fit_series.index, method="ffill")
        )
        if spike_series.isna().all():
            warnings.warn(
                "exclude_spike=True but no spike_status data aligned with fit "
                "timestamps; spike filter was not applied.",
                UserWarning,
                stacklevel=2,
            )
        else:
            allowed_mask = (
                allowed_mask
                & (spike_series != "spike")
                & (spike_series != "potential")
            )

    allowed_candidates = fit_series[allowed_mask]
    if allowed_candidates.empty:
        return pd.DataFrame()

    # --- Pre-compute loop constants ---
    window_duration = pd.Timedelta(minutes=window_minutes)
    window_duration_sec = float(window_minutes * 60)
    # Nanosecond integer offset for the window (avoids per-iteration Timedelta arithmetic)
    window_ns = np.int64(window_minutes * 60 * 1_000_000_000)

    # Pre-built nanosecond array over the full fit index for searchsorted position lookups.
    # All enrichment series share this same index (reindexed above), so a single pair of
    # positions covers every iloc slice in the loop.
    fit_ns_array = fit_series.index.asi8

    # Map each unique calendar date to its period-end as a nanosecond int
    if period == "morning":
        period_delta = pd.Timedelta(hours=morning_end_hour)
    elif period == "evening":
        period_delta = pd.Timedelta(days=1)  # evening ends at midnight
    else:  # daytime
        period_delta = pd.Timedelta(hours=daytime_end_hour)

    unique_dates = allowed_candidates.index.normalize().unique()
    period_end_ns_map: dict = {
        d: int((d + period_delta).value) for d in unique_dates
    }

    # Pre-compute normalized dates for all candidates in one vectorised call
    # (avoids a per-iteration scalar .normalize() allocation)
    candidate_dates = allowed_candidates.index.normalize()

    candidate_rows: list[dict] = []

    for (timestamp, fit_price), period_date in zip(
        allowed_candidates.items(), candidate_dates
    ):
        ts_ns_scalar = np.int64(timestamp.value)
        window_end_ns = ts_ns_scalar + window_ns
        period_end_ns = period_end_ns_map[period_date]

        if window_end_ns > period_end_ns:
            continue

        # Integer-position slices via searchsorted — reduces binary searches from
        # ~10 per iteration (one per .loc call) to 2, and eliminates end_slice fencing.
        start_pos = int(fit_ns_array.searchsorted(ts_ns_scalar))
        end_pos = int(fit_ns_array.searchsorted(window_end_ns))  # exclusive upper bound

        window_fit = fit_series.iloc[start_pos:end_pos]
        if len(window_fit) < min_samples:
            continue

        # Time-weighted average via nanosecond arithmetic
        ts_ns = fit_ns_array[start_pos:end_pos]
        durations_sec = np.empty(len(ts_ns), dtype="float64")
        if len(ts_ns) > 1:
            durations_sec[:-1] = (ts_ns[1:] - ts_ns[:-1]) / 1e9
        durations_sec[-1] = (window_end_ns - ts_ns[-1]) / 1e9

        avg_fit = (window_fit.values * durations_sec).sum() / window_duration_sec

        row: dict = {
            "start_time": timestamp,
            "window_end_time": timestamp + window_duration,
            "fit_at_start": float(fit_price),
            "avg_fit_next_hour": avg_fit,
            "min_fit_next_hour": float(window_fit.min()),
            "max_fit_next_hour": float(window_fit.max()),
            "fit_std_next_hour": float(window_fit.std(ddof=1)),
            "samples_in_window": int(window_fit.shape[0]),
        }

        # Enrichment: general tariff
        if "general" in enrichment:
            gen_val = enrichment["general"].at[timestamp]
            if not pd.isna(gen_val):
                row["general_at_start"] = float(gen_val)
                row["fit_vs_general"] = float(fit_price) - float(gen_val)

        # Enrichment: renewables (simple mean — sampling grid is uniform after expansion)
        if "renewables" in enrichment:
            window_ren = enrichment["renewables"].iloc[start_pos:end_pos]
            if not window_ren.empty:
                avg_ren = float(window_ren.mean())
                if not pd.isna(avg_ren):
                    row["avg_renewables"] = avg_ren

        # Enrichment: confidence interval (time-weighted, consistent with avg_fit).
        # Guard against a length mismatch if leading NaNs survive ffill in the
        # enrichment series — reusing durations_sec with a differently-shaped array
        # would raise a ValueError.
        if "predicted_low" in enrichment:
            window_low = enrichment["predicted_low"].iloc[start_pos:end_pos]
            window_high = enrichment["predicted_high"].iloc[start_pos:end_pos]
            if (
                len(window_low) == len(window_fit)
                and not window_low.empty
                and not window_high.empty
            ):
                row["conf_low"] = float(
                    (window_low.values * durations_sec).sum() / window_duration_sec
                )
                row["conf_high"] = float(
                    (window_high.values * durations_sec).sum() / window_duration_sec
                )

        # Enrichment: estimate flag
        if "is_estimate" in enrichment:
            window_est = enrichment["is_estimate"].iloc[start_pos:end_pos]
            if not window_est.empty:
                row["has_estimate"] = bool(np.any(window_est))

        # Composite score: price + renewables contribution.
        # renewables_weight is in the same units as FIT prices per 1 % renewables,
        # so at 100 % renewables the bonus equals renewables_weight * 100.
        row["_composite"] = avg_fit + renewables_weight * row.get("avg_renewables", 0.0)

        candidate_rows.append(row)

    if not candidate_rows:
        warnings.warn(
            f"No valid {period} export windows found "
            f"(window_minutes={window_minutes}, min_samples={min_samples}). "
            "Try reducing min_samples or window_minutes.",
            UserWarning,
            stacklevel=2,
        )
        return pd.DataFrame()

    result = (
        pd.DataFrame(candidate_rows)
        .sort_values(
            by=["_composite", "fit_at_start", "start_time"],
            ascending=[False, False, True],
        )
        .head(top_n)
        .drop(columns=["_composite"], errors="ignore")
        .reset_index(drop=True)
    )

    # Pin column order for consistent output regardless of which enrichment
    # columns are present
    result = result.reindex(columns=[c for c in _COLUMN_ORDER if c in result.columns])

    # Defer rounding to post-sort so only top_n rows are rounded
    for _col, _dec in [
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
    ]:
        if _col in result.columns:
            result[_col] = result[_col].round(_dec)

    return result
