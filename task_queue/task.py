"""
Celery task definitions for the insurance scraper pipeline.
This module defines the Celery app and the scrape_car task that
drives the full scraping + DB-sync workflow.
"""

import asyncio
import logging
import time
import sys
import psycopg2
from pathlib import Path
from typing import Optional

from celery import Celery, Task
from celery.exceptions import SoftTimeLimitExceeded

from task_queue.configure import (
    CELERY_BROKER_URL,
    CELERY_RESULT_BACKEND,
    CELERY_TASK_TIME_LIMIT,
    CELERY_TASK_SOFT_TIME_LIMIT,
    CELERY_TASK_MAX_RETRIES,
    CELERY_TASK_DEFAULT_RETRY_DELAY,
    DB_CONFIG,
    LOG_LEVEL,
)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# ── Celery app ───────────────────────────────────────────────────────────────
app = Celery(
    "insurance_scraper",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Kolkata",
    enable_utc=True,
    task_time_limit=CELERY_TASK_TIME_LIMIT,
    task_soft_time_limit=CELERY_TASK_SOFT_TIME_LIMIT,
    result_expires=24 * 60 * 60,
    task_track_started=True,
)


# ── DB helpers ───────────────────────────────────────────────────────────────

def _update_run_status(run_id, car_number, status, error=None):
    """Update scrape_runs row status. Non-fatal — logs and continues on error."""
    try:
        from datetime import datetime
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()
        now  = datetime.now()
        cur.execute(
            """
            UPDATE scrape_runs
               SET status     = %s,
                   ended_at   = %s,
                   notes      = %s,
                   updated_at = %s
             WHERE run_id = %s
            """,
            (
                status,
                now,
                f"Car: {car_number}" + (f" | Error: {error}" if error else ""),
                now,
                str(run_id),
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        logger.error("DB status update failed for run_id=%s: %s", run_id, exc)


# ── Base task class ───────────────────────────────────────────────────────────

class CallbackTask(Task):
    max_retries         = CELERY_TASK_MAX_RETRIES
    default_retry_delay = CELERY_TASK_DEFAULT_RETRY_DELAY

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        run_id     = kwargs.get("run_id") or (args[0] if args else "UNKNOWN")
        car_number = kwargs.get("car_number", "UNKNOWN")
        _update_run_status(run_id, car_number, "FAIL", str(exc))
        logger.error("Task %s FAILED for %s: %s", task_id, car_number, exc)

    def on_success(self, retval, task_id, args, kwargs):
        car_number = kwargs.get("car_number", "UNKNOWN")
        logger.info("Task %s SUCCESS for %s", task_id, car_number)


# ── scrape_car task ───────────────────────────────────────────────────────────

@app.task(
    bind=True,
    base=CallbackTask,
    max_retries=CELERY_TASK_MAX_RETRIES,
    default_retry_delay=CELERY_TASK_DEFAULT_RETRY_DELAY,
    queue="celery",
    name="scrape_car",
)
def scrape_car(
    self,
    car_number: str,
    run_id=None,
    cust_name: Optional[str] = None,
    phone: Optional[str] = None,
    policy_expiry: Optional[str] = None,
    claim_status: Optional[str] = None,
    user_profile_dir: Optional[str] = None,
):
    """
    Scrape insurance quotes for a car and sync results to the database.

    Args:
        car_number:       Vehicle registration number (with_reg) or NOREG identifier (without_reg)
        run_id:           ScrapeRun UUID — links this task to the DB record
        cust_name:        Customer name (optional)
        phone:            Customer phone (optional)
        policy_expiry:    Policy expiry status (optional)
        claim_status:     Claim status (optional)
        user_profile_dir: Browser profile directory (optional)
    """
    logger.info("=" * 70)
    logger.info("TASK STARTED  run_id=%s  car=%s", run_id, car_number)
    logger.info("=" * 70)

    conn = None
    try:
        start_time = time.time()

        # Ensure pb_scraper is importable
        project_root   = Path(__file__).resolve().parent.parent
        pb_scripts_path = str(project_root / "backend" / "celery" / "pb_scraper")
        for p in (str(project_root), pb_scripts_path):
            if p not in sys.path:
                sys.path.insert(0, p)

        try:
            from backend.celery.pb_scraper.cmf_locator_v2 import run as run_scraper
        except ImportError as exc:
            logger.error("Could not import scraper from %s: %s", pb_scripts_path, exc)
            raise

        # Open DB connection for live sync
        conn    = psycopg2.connect(**DB_CONFIG)
        from backend.celery.db_and_logging.db_live_sync import LiveDBSync
        db_sync = LiveDBSync(run_id, conn)

        # Phase 1 — scraping
        logger.info("[1/3] Scraping %s …", car_number)
        self.update_state(state="PROGRESS", meta={"current": 0, "total": 100, "message": "Starting browser…"})

        asyncio.run(
            run_scraper(
                run_id=run_id,
                car_number=car_number,
                car_name="NEW_CAR",
                cust_name=cust_name,
                phone=phone,
                policy_expiry=policy_expiry,
                claim_status=claim_status,
                user_profile_dir=user_profile_dir,
            )
        )
        logger.info("[1/3] Scraping done.")

        # Phase 2 — save responses
        logger.info("[2/3] Saving responses …")
        self.update_state(state="PROGRESS", meta={"current": 50, "total": 100, "message": "Saving responses…"})

        # Phase 3 — finalise run record
        logger.info("[3/3] Finalising run record …")
        self.update_state(state="PROGRESS", meta={"current": 90, "total": 100, "message": "Finalising…"})

        duration_ms = int((time.time() - start_time) * 1000)
        db_sync.finalize_run(
            run_id=run_id,
            status="SUCCESS",
            total_duration_ms=duration_ms,
            notes=f"Car: {car_number} | Phone: {phone or 'N/A'}",
        )

        logger.info("TASK COMPLETED  run_id=%s  duration=%d ms", run_id, duration_ms)
        return {
            "status":      "SUCCESS",
            "car_number":  car_number,
            "run_id":      str(run_id),
            "duration_ms": duration_ms,
        }

    except SoftTimeLimitExceeded:
        logger.warning("Soft timeout for %s", car_number)
        _update_run_status(run_id, car_number, "FAIL", "Soft time limit exceeded")
        raise self.retry(countdown=CELERY_TASK_DEFAULT_RETRY_DELAY)

    except Exception as exc:
        logger.error("Error for %s: %s", car_number, exc, exc_info=True)
        raise self.retry(countdown=CELERY_TASK_DEFAULT_RETRY_DELAY * (self.request.retries + 1))

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ── health_check task ─────────────────────────────────────────────────────────

@app.task(bind=True, base=CallbackTask, name="health_check")
def health_check(self):
    """Lightweight liveness probe for the worker."""
    logger.info("Health check OK")
    return {"status": "HEALTHY"}


if __name__ == "__main__":
    app.start()
