"""
Structured run logger for scan jobs.

Each scan run gets a UUID.  Per-ticker timing and provider outcomes are
tracked in memory and persisted to:

  - SQLite ``ticker_metrics`` table (via scanner.py)
  - A newline-delimited JSON file at ``<log_dir>/scan_<YYYYMMDD>_<run_id[:8]>.jsonl``

The regular Python ``logging`` handlers continue to write human-readable
logs to stdout; this module provides the machine-readable complement.
"""

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


class ScanRunLogger:
    """Track timing and outcomes for a single scan run."""

    def __init__(self, universe: str, log_dir: Optional[Path] = None):
        self.run_id: str = str(uuid.uuid4())
        self.universe: str = universe
        self._wall_start: str = datetime.now(timezone.utc).isoformat()
        self._mono_start: float = time.monotonic()

        # {ticker: float}  — monotonic start time
        self._ticker_start: dict[str, float] = {}

        # {ticker: {provider, success, duration_s, score}}
        self.ticker_outcomes: dict[str, dict] = {}

        # {provider: {attempts, successes, failures, total_duration_s}}
        self.provider_stats: dict[str, dict] = {}

        self._jsonl: Optional[object] = None  # file handle
        if log_dir is not None:
            self._open_jsonl(Path(log_dir))

        self._emit({"event": "scan_start", "universe": universe})

    # ── Public API ──────────────────────────────────────────────────────────

    def start_ticker(self, ticker: str) -> None:
        self._ticker_start[ticker] = time.monotonic()

    def end_ticker(
        self,
        ticker: str,
        provider: str,
        success: bool,
        score: Optional[int] = None,
    ) -> None:
        duration = time.monotonic() - self._ticker_start.get(ticker, time.monotonic())
        self.ticker_outcomes[ticker] = {
            "provider": provider,
            "success": success,
            "duration_s": round(duration, 2),
            "score": score,
        }
        self._update_provider_stats(provider, success, duration)
        self._emit({
            "event": "ticker_done",
            "ticker": ticker,
            "provider": provider,
            "success": success,
            "duration_s": round(duration, 2),
            "score": score,
        })

    def summary(self) -> dict:
        elapsed = time.monotonic() - self._mono_start
        scanned = sum(1 for o in self.ticker_outcomes.values() if o["success"])
        failed = sum(1 for o in self.ticker_outcomes.values() if not o["success"])
        return {
            "run_id": self.run_id,
            "universe": self.universe,
            "started_wall": self._wall_start,
            "duration_seconds": round(elapsed, 1),
            "stocks_scanned": scanned,
            "stocks_failed": failed,
            "provider_stats": self.provider_stats,
        }

    def finish(self) -> None:
        """Emit a summary event and close the JSON-lines file."""
        s = self.summary()
        self._emit({"event": "scan_complete", **s})
        if self._jsonl:
            try:
                self._jsonl.close()
            except Exception:
                pass
            self._jsonl = None

    # ── Internal ─────────────────────────────────────────────────────────────

    def _update_provider_stats(self, provider: str, success: bool, duration: float) -> None:
        if provider not in self.provider_stats:
            self.provider_stats[provider] = {
                "attempts": 0,
                "successes": 0,
                "failures": 0,
                "total_duration_s": 0.0,
            }
        s = self.provider_stats[provider]
        s["attempts"] += 1
        s["successes" if success else "failures"] += 1
        s["total_duration_s"] = round(s["total_duration_s"] + duration, 2)

    def _open_jsonl(self, log_dir: Path) -> None:
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
            date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
            path = log_dir / f"scan_{date_str}_{self.run_id[:8]}.jsonl"
            self._jsonl = open(path, "w", buffering=1)  # line-buffered
            log.debug("Structured log: %s", path)
        except Exception as exc:
            log.warning("Could not open JSON-lines log: %s", exc)
            self._jsonl = None

    def _emit(self, data: dict) -> None:
        if not self._jsonl:
            return
        try:
            record = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "run_id": self.run_id,
                **data,
            }
            self._jsonl.write(json.dumps(record) + "\n")
        except Exception:
            pass
