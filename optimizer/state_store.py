from __future__ import annotations

import json
import sqlite3
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


class StateStore:
    def __init__(self, db_path: str) -> None:
        path = Path(db_path).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._lock = threading.Lock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kv_state (
                  key TEXT PRIMARY KEY,
                  value_json TEXT NOT NULL,
                  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS price_tracking (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts TEXT NOT NULL,
                  block_ts TEXT NOT NULL,
                  grid_import_kw REAL NOT NULL DEFAULT 0.0,
                  grid_export_kw REAL NOT NULL DEFAULT 0.0,
                  import_price REAL,
                  feedin_price REAL,
                  battery_soc REAL
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pt_block_ts ON price_tracking(block_ts)"
            )
            self._conn.commit()

    def set_json(self, key: str, value: dict[str, Any]) -> None:
        payload = json.dumps(value, separators=(",", ":"), sort_keys=True)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO kv_state(key, value_json, updated_at)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(key) DO UPDATE SET
                  value_json = excluded.value_json,
                  updated_at = datetime('now')
                """,
                (key, payload),
            )
            self._conn.commit()

    def get_json(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT value_json FROM kv_state WHERE key = ?", (key,)).fetchone()
        if not row:
            return None
        try:
            data = json.loads(row[0])
        except json.JSONDecodeError:
            return None
        return data if isinstance(data, dict) else None

    # ------------------------------------------------------------------
    # Price tracking
    # ------------------------------------------------------------------

    def purge_day(self, date: str) -> int:
        """Delete all price_tracking rows whose block_ts falls on *date* (YYYY-MM-DD).
        Returns the number of rows deleted."""
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM price_tracking WHERE block_ts LIKE ?",
                (f"{date}%",),
            )
            self._conn.commit()
            return cur.rowcount

    def purge_old_price_tracking(self, retain_days: int = 7) -> int:
        """Delete price_tracking rows older than retain_days. Returns number of rows deleted."""
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM price_tracking WHERE ts < datetime('now', ?)",
                (f"-{retain_days} days",),
            )
            self._conn.commit()
            return cur.rowcount

    def record_price_event(
        self,
        ts: str,
        block_ts: str,
        grid_import_kw: float,
        grid_export_kw: float,
        import_price: float | None,
        feedin_price: float | None,
        battery_soc: float | None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO price_tracking
                  (ts, block_ts, grid_import_kw, grid_export_kw, import_price, feedin_price, battery_soc)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (ts, block_ts, grid_import_kw, grid_export_kw, import_price, feedin_price, battery_soc),
            )
            self._conn.commit()

    def get_price_events(self, date: str | None = None, limit: int = 2000) -> list[dict[str, Any]]:
        """Return price tracking records in ascending time order.
        date='YYYY-MM-DD' filters to that local date (matched via block_ts prefix)."""
        with self._lock:
            if date:
                rows = self._conn.execute(
                    """SELECT ts, block_ts, grid_import_kw, grid_export_kw,
                              import_price, feedin_price, battery_soc
                       FROM price_tracking
                       WHERE block_ts LIKE ?
                       ORDER BY ts ASC LIMIT ?""",
                    (f"{date}%", limit),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    """SELECT ts, block_ts, grid_import_kw, grid_export_kw,
                              import_price, feedin_price, battery_soc
                       FROM price_tracking
                       ORDER BY ts DESC LIMIT ?""",
                    (limit,),
                ).fetchall()
        return [
            {
                "ts": r[0],
                "block_ts": r[1],
                "grid_import_kw": r[2],
                "grid_export_kw": r[3],
                "import_price": r[4],
                "feedin_price": r[5],
                "battery_soc": r[6],
            }
            for r in rows
        ]

    def daily_earnings_summary(self, date: str) -> dict[str, Any]:
        """Aggregate price_tracking records for a date ('YYYY-MM-DD') into per-block
        energy and cost/revenue totals using time-weighted averaging within each
        5-minute billing block."""
        events = self.get_price_events(date=date, limit=20000)
        if not events:
            return {
                "date": date,
                "total_import_kwh": 0.0,
                "total_export_kwh": 0.0,
                "import_costs": 0.0,
                "export_earnings": 0.0,
                "net": 0.0,
                "blocks": [],
            }

        # Group ascending records by block_ts
        by_block: dict[str, list[dict]] = defaultdict(list)
        for e in events:
            by_block[e["block_ts"]].append(e)

        block_summaries = []
        total_import_kwh = 0.0
        total_export_kwh = 0.0
        total_import_costs = 0.0   # net: negative = earned (paid to consume at negative prices)
        total_export_earnings = 0.0  # net: negative = charged (exported during negative feedin)

        for block_ts in sorted(by_block):
            recs = sorted(by_block[block_ts], key=lambda x: x["ts"])
            try:
                block_start = datetime.fromisoformat(block_ts)
            except ValueError:
                continue
            block_end = block_start + timedelta(minutes=5)

            weighted_import = 0.0
            weighted_export = 0.0
            total_weight = 0.0
            block_import_price: float | None = None
            block_feedin_price: float | None = None

            for i, rec in enumerate(recs):
                try:
                    rec_ts = datetime.fromisoformat(rec["ts"])
                except ValueError:
                    continue
                next_ts = datetime.fromisoformat(recs[i + 1]["ts"]) if i + 1 < len(recs) else block_end
                # For the first record, extend back to block_start — the runtime poll
                # fires a few seconds after the block boundary so rec_ts > block_start;
                # using block_start prevents silently losing that gap each block.
                seg_start = block_start if i == 0 else rec_ts
                seg_end = min(next_ts, block_end)
                duration = (seg_end - seg_start).total_seconds()
                if duration <= 0:
                    continue
                weighted_import += rec["grid_import_kw"] * duration
                weighted_export += rec["grid_export_kw"] * duration
                total_weight += duration
                # Use the last non-null price in the block: Amber's price sensor updates
                # slightly after the block boundary, so the first record may still carry
                # the previous block's price.  Overwriting gives us the settled value.
                if rec["import_price"] is not None:
                    block_import_price = rec["import_price"]
                if rec["feedin_price"] is not None:
                    block_feedin_price = rec["feedin_price"]

            if total_weight > 0:
                avg_import_kw = weighted_import / total_weight
                avg_export_kw = weighted_export / total_weight
                block_h = total_weight / 3600.0
            else:
                avg_import_kw = recs[0]["grid_import_kw"]
                avg_export_kw = recs[0]["grid_export_kw"]
                block_h = 5.0 / 60.0

            import_kwh = avg_import_kw * block_h
            export_kwh = avg_export_kw * block_h
            ip = block_import_price or 0.0
            fp = block_feedin_price or 0.0
            block_import_cost = import_kwh * ip        # negative when price < 0
            block_export_earning = export_kwh * fp     # negative when feedin < 0

            total_import_kwh += import_kwh
            total_export_kwh += export_kwh
            total_import_costs += block_import_cost
            total_export_earnings += block_export_earning

            block_summaries.append({
                "block_ts": block_ts,
                "import_kwh": round(import_kwh, 4),
                "export_kwh": round(export_kwh, 4),
                "import_price": block_import_price,
                "feedin_price": block_feedin_price,
                "import_costs": round(block_import_cost, 4),
                "export_earnings": round(block_export_earning, 4),
                "net": round(block_export_earning - block_import_cost, 4),
            })

        net = total_export_earnings - total_import_costs
        return {
            "date": date,
            "total_import_kwh": round(total_import_kwh, 3),
            "total_export_kwh": round(total_export_kwh, 3),
            "import_costs": round(total_import_costs, 4),
            "export_earnings": round(total_export_earnings, 4),
            "net": round(net, 4),
            "blocks": block_summaries,
        }
