"""
Startup environment check.
Logs the configuration status of every recognised env var and warns clearly
when the application is running in a degraded configuration.
"""

import logging
import os
import sys

log = logging.getLogger(__name__)

# Variables that enable optional features; absence degrades but does not break.
OPTIONAL_VARS = {
    "FINNHUB_API_KEY": "Finnhub provider (extra fundamentals)",
    "FMP_API_KEY": "Financial Modelling Prep provider (extra fundamentals)",
    "ALPHA_VANTAGE_API_KEY": "Alpha Vantage provider (extra fundamentals)",
    "OLLAMA_HOST": "Ollama AI narrative generation",
    "OLLAMA_MODEL": "Ollama model selection",
}


def check_env(*, exit_on_error: bool = False) -> bool:
    """
    Validate the runtime environment and log a clear status summary.

    Returns True if the environment is fully configured, False if degraded.
    Calls sys.exit(1) only when exit_on_error=True and a hard failure is found.
    """
    universe = os.environ.get("UNIVERSE", "asx200")
    log.info("Universe: %s (override via UNIVERSE env var)", universe)

    missing_optional: list[str] = []

    for var, description in OPTIONAL_VARS.items():
        if not os.environ.get(var):
            missing_optional.append(f"  {var:30s}  ({description})")

    # YahooQuery is always available (no key required), so the app can always
    # start.  We only need to warn, not fail.
    if missing_optional:
        log.warning(
            "The following optional env vars are not set — "
            "related features will be disabled:"
        )
        for line in missing_optional:
            log.warning(line)
        log.warning(
            "YahooQuery is active as the primary free provider. "
            "Add keys to .env to enable additional data sources."
        )
        return False

    log.info("All optional providers configured.")
    return True
