"""
Universe loader — reads config/universe.yaml and returns a named ticker list.

Priority for resolving which universe to use:
  1. The ``name`` argument passed directly to get_universe()
  2. The UNIVERSE environment variable
  3. The ``active`` key in the YAML file
  4. Hard-coded fallback (asx200 from the YAML, or an empty list)
"""

import logging
import os
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


def _find_config() -> Path:
    """
    Find universe.yaml relative to this file, handling both Docker
    (/app/config/) and local-dev (repo root config/) layouts.
    """
    # Docker layout: Dockerfile copies config/ → /app/config/
    local = Path(__file__).parent / "config" / "universe.yaml"
    if local.exists():
        return local
    # Local-dev layout: repo_root/config/universe.yaml
    repo = Path(__file__).parent.parent / "config" / "universe.yaml"
    if repo.exists():
        return repo
    return local  # return the preferred path even if absent (caller handles it)


def _load(config_path: Optional[Path] = None) -> dict:
    path = config_path or _find_config()
    try:
        import yaml  # pyyaml
        with open(path) as f:
            return yaml.safe_load(f) or {}
    except ImportError:
        log.error("pyyaml is not installed — cannot load universe config")
        return {}
    except FileNotFoundError:
        log.warning("Universe config not found at %s", path)
        return {}
    except Exception as exc:
        log.error("Failed to load universe config: %s", exc)
        return {}


def get_universe(
    name: Optional[str] = None,
    config_path: Optional[Path] = None,
) -> tuple[str, list[str]]:
    """
    Return ``(display_name, ticker_list)`` for the resolved universe.

    Falls back gracefully at each step so the scanner can always run.
    """
    config = _load(config_path)
    universes: dict = config.get("universes", {})

    target = (
        name
        or os.environ.get("UNIVERSE", "").strip()
        or config.get("active", "asx200")
    )

    if target in universes:
        entry = universes[target]
        tickers: list[str] = entry.get("tickers") or []
        display = entry.get("name", target)
        if tickers:
            log.info("Universe '%s' (%s): %d tickers", target, display, len(tickers))
            return display, tickers
        log.warning("Universe '%s' has no tickers — falling back to asx200", target)

    # Fallback: try asx200 from config
    if "asx200" in universes:
        entry = universes["asx200"]
        tickers = entry.get("tickers") or []
        display = entry.get("name", "ASX 200")
        log.warning("Using fallback universe '%s' (%d tickers)", display, len(tickers))
        return display, tickers

    log.error("No usable universe found in config — returning empty list")
    return "Unknown", []


def list_universes(config_path: Optional[Path] = None) -> dict[str, dict]:
    """
    Return a summary dict of every defined universe, keyed by universe ID.

    Each value is ``{name, description, count}``.
    """
    config = _load(config_path)
    active = config.get("active", "")
    return {
        key: {
            "name": u.get("name", key),
            "description": u.get("description", ""),
            "count": len(u.get("tickers") or []),
            "active": key == active,
        }
        for key, u in config.get("universes", {}).items()
    }
