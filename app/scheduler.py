"""
Scheduler - runs the nightly scan at 8pm AWST (12:00 UTC) every day.
Also runs an immediate scan on first startup if no data exists.
"""

import time
import logging
import sqlite3
from datetime import datetime, date
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

DB_PATH = Path("/data/stocks.db")
AWST = pytz.timezone("Australia/Perth")


def run_scan_job():
    log.info("Scheduler triggered scan")
    try:
        from scanner import run_scan
        run_scan()
    except Exception as e:
        log.error(f"Scan failed: {e}")


def has_data_today():
    if not DB_PATH.exists():
        return False
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT COUNT(*) as c FROM scores WHERE scan_date = ?",
            (date.today().isoformat(),)
        ).fetchone()
        conn.close()
        return row[0] > 0
    except Exception:
        return False


def main():
    log.info("Starting scheduler...")

    # Run immediately if no data for today
    if not has_data_today():
        log.info("No data for today - running initial scan now")
        run_scan_job()
    else:
        log.info("Data already exists for today - skipping initial scan")

    # Schedule nightly at 20:00 AWST
    scheduler = BackgroundScheduler(timezone=AWST)
    scheduler.add_job(
        run_scan_job,
        CronTrigger(hour=20, minute=0, timezone=AWST),
        id="nightly_scan",
        name="Nightly ASX scan",
        replace_existing=True,
    )
    scheduler.start()
    log.info("Scheduler started - nightly scan at 20:00 AWST")

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        log.info("Scheduler stopped")


if __name__ == "__main__":
    main()
