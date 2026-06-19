"""
Celery Tasks with Direct Database Integration
Automatically saves scraped responses to PostgreSQL on completion
"""

import asyncio
import logging
import time
import psycopg2
import json
import os
import sys
import ssl
from pathlib import Path
from datetime import datetime
from typing import Optional
from celery import Celery, Task
from celery.exceptions import SoftTimeLimitExceeded

from backend.celery.task_queue.config import (
    REDIS_URL,
    CELERY_BROKER_URL,
    CELERY_RESULT_BACKEND,
    CELERY_TASK_TIME_LIMIT,
    CELERY_TASK_SOFT_TIME_LIMIT,
    CELERY_TASK_MAX_RETRIES,
    CELERY_TASK_DEFAULT_RETRY_DELAY,
    DB_CONFIG,
    LOG_LEVEL,
)

from backend.celery.db_and_logging.db_live_sync import LiveDBSync

# ============================================================================
# CELERY APP
# ============================================================================
app = Celery(
    "insurance_scraper",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
)

_ssl_opts = {"ssl_cert_reqs": ssl.CERT_NONE} if CELERY_BROKER_URL.startswith("rediss://") else {}

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
    broker_use_ssl=_ssl_opts or None,
    redis_backend_use_ssl=_ssl_opts or None,
    beat_schedule={
        "pipeline-catchup-every-5-min": {
            "task": "pipeline_catchup",
            "schedule": 300.0,  # every 5 minutes
        },
    },
)

# ============================================================================
# LOGGING SETUP
# ============================================================================
logger = logging.getLogger(__name__)
logging.basicConfig(level=LOG_LEVEL)


# ============================================================================
# DATABASE HELPERS
# ============================================================================
def insert_scrape_run_to_db(run_id, car_number, status, error=None):
    """Insert task result into scrape_runs table"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        now = datetime.now()
        
        cursor.execute("""
            INSERT INTO scrape_runs (
                run_id, 
                status, 
                started_at, 
                ended_at, 
                total_duration_ms, 
                notes, 
                created_at, 
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                status = EXCLUDED.status,
                ended_at = EXCLUDED.ended_at,
                notes = EXCLUDED.notes,
                updated_at = EXCLUDED.updated_at
        """, (
            run_id,
            status,
            now,
            now if status in ['SUCCESS', 'FAILURE'] else None,
            None,
            f"Car: {car_number}" + (f" | Error: {error}" if error else ""),
            now,
            now,
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Inserted run_id={run_id} into scrape_runs table")
        return True
    except Exception as e:
        logger.error(f"❌ Database error inserting scrape_runs: {e}")
        return False


def save_scraped_data_to_db(run_id, car_number, data_dir):
    """Automatically save all scraped response data to quotes_responses table"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        logger.info(f"📂 Looking for scraped data in: {data_dir}")
        
        if not os.path.exists(data_dir):
            logger.warning(f"⚠️  Data directory not found: {data_dir}")
            return False
        
        json_files = list(Path(data_dir).glob("*.json"))
        if not json_files:
            logger.warning(f"⚠️  No JSON files found in {data_dir}")
            return False
        
        logger.info(f"📊 Found {len(json_files)} JSON files to save")
        inserted_count = 0
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    response_data = json.load(f)
                
                api_name = json_file.stem
                
                cursor.execute("""
                    INSERT INTO quotes_responses (
                        run_id,
                        api_name,
                        api_url,
                        response_json,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    run_id,
                    api_name,
                    None,
                    json.dumps(response_data),
                    datetime.now(),
                    datetime.now(),
                ))
                
                inserted_count += 1
                logger.info(f"   ✓ Saved {api_name} → quotes_responses")
            
            except json.JSONDecodeError as e:
                logger.warning(f"⚠️  Invalid JSON in {json_file.name}: {e}")
            except Exception as e:
                logger.error(f"❌ Error processing {json_file.name}: {e}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"💾 Saved {inserted_count} responses to DB (run_id={run_id})")
        return inserted_count > 0
    
    except Exception as e:
        logger.error(f"❌ Database save error: {e}")
        return False


# ============================================================================
# BASE TASK CLASS
# ============================================================================
class CallbackTask(Task):
    autoretry_for = (Exception,)
    max_retries = CELERY_TASK_MAX_RETRIES
    default_retry_delay = CELERY_TASK_DEFAULT_RETRY_DELAY
    
    def on_failure(self, exc, run_id, args, kwargs, einfo):
        car_number = args[0] if args else "UNKNOWN"
        insert_scrape_run_to_db(run_id, car_number, "FAILURE", str(exc))
        logger.error(f"❌ Task {run_id} FAILED for {car_number}: {exc}")
    
    def on_success(self, result, run_id, args, kwargs):
        car_number = args[0] if args else "UNKNOWN"
        logger.info(f"✅ Task {run_id} SUCCESS for {car_number}")


# ============================================================================
# SCRAPER TASK
# ============================================================================

@app.task(
    bind=True,
    base=CallbackTask,
    max_retries=CELERY_TASK_MAX_RETRIES,
    default_retry_delay=CELERY_TASK_DEFAULT_RETRY_DELAY,
    queue='celery',
    name='scrape_car'
)
def scrape_car(
    self,
    run_id,
    car_brand: Optional[str] = None,
    car_model: Optional[str] = None,
    fuel_type: Optional[str] = None,
    variant: Optional[str] = None,
    year: Optional[str] = None,
    rto_code: Optional[str] = None,
    ncb_percent: Optional[str] = None,
    cust_name: Optional[str] = None,
    phone: Optional[str] = None,
    policy_expiry: Optional[str] = None,
    claim_status: Optional[str] = None,
    quotes_url: Optional[str] = None,
    user_profile_dir: Optional[str] = None,
    profile_unique_key: Optional[str] = None,
    profile_identifier_key: Optional[str] = None,
):

    car_label = f"{car_brand or ''} {car_model or ''} {variant or ''}".strip() or run_id

    logger.info(f"\n{'='*70}")
    logger.info(f" TASK STARTED: {run_id}")
    logger.info(f" Car: {car_label} | RTO: {rto_code} | Year: {year} | Fuel: {fuel_type}")
    logger.info(f"{'='*70}")


    try:
        start_time = time.time()

        # Add paths
        project_root = Path(__file__).parent.parent
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))

        pb_scripts_path = str(project_root / "pb_scraper")
        if pb_scripts_path not in sys.path:
            sys.path.insert(0, pb_scripts_path)

        try:
            from cmf_locator_v6 import run as run_scraper
        except ImportError:
            logger.error(f"❌ Could not import cmf_locator_v6 from {project_root}")
            raise

        # Initialize DB sync
        conn = psycopg2.connect(**DB_CONFIG)
        db_sync = LiveDBSync(run_id, conn)

        # Phase 1: Run scraper
        logger.info(f"\n[PHASE 1/3] 🌐 SCRAPING...")
        self.update_state(
            state='PROGRESS',
            meta={'current': 0, 'total': 100, 'message': 'Starting browser...'}
        )

        asyncio.run(
            run_scraper(
                run_id=run_id,
                quotes_url=quotes_url,
                car_brand=car_brand,
                car_model=car_model,
                fuel_type=fuel_type,
                variant=variant,
                year=year,
                rto_code=rto_code,
                cust_name=cust_name,
                phone=phone,
                policy_expiry=policy_expiry,
                claim_status=claim_status,
                ncb_percent=ncb_percent,
                profile_unique_key=profile_unique_key,
                profile_identifier_key=profile_identifier_key,
            )
        )
        logger.info(f"✓ Scraping completed")

        # Phase 2: Save responses to database
        logger.info(f"\n[PHASE 2/3] 💾 SAVING TO DATABASE...")
        self.update_state(
            state='PROGRESS',
            meta={'current': 50, 'total': 100, 'message': 'Saving responses...'}
        )


        # Phase 3: Finalize run record
        logger.info(f"\n[PHASE 3/3] 📝 RECORDING RUN...")
        self.update_state(
            state='PROGRESS',
            meta={'current': 90, 'total': 100, 'message': 'Finalizing...'}
        )

        total_duration_ms = int((time.time() - start_time) * 1000)
        db_sync.finalize_run(
            run_id=run_id,
            status="SUCCESS",
            total_duration_ms=total_duration_ms,
            notes=f"Car: {car_label} | Phone: {phone or 'N/A'}",
        )

        result = {
            "status": "SUCCESS",
            "car_label": car_label,
            "task_id": run_id,
            "duration_ms": total_duration_ms,
        }

        logger.info(f"\n{'='*70}")
        logger.info(f"✅ TASK COMPLETED: {run_id}")
        logger.info(f"⏱️ Duration: {total_duration_ms}ms")
        logger.info(f"{'='*70}\n")

        return result

    except SoftTimeLimitExceeded:
        logger.warning(f"⏱️  Soft timeout for {car_label}")
        insert_scrape_run_to_db(run_id, car_label, "TIMEOUT", "Soft time limit exceeded")
        raise self.retry(countdown=CELERY_TASK_DEFAULT_RETRY_DELAY)
    
    except Exception as exc:
        logger.error(f"❌ Error: {exc}", exc_info=True)
        raise self.retry(countdown=CELERY_TASK_DEFAULT_RETRY_DELAY * (self.request.retries + 1))

    finally:
        if 'conn' in locals():
            try:
                conn.close()
            except Exception:
                pass


# ============================================================================
# DUMMY TEST TASK — separate queue, no real scraping, no DB writes
# ============================================================================

@app.task(
    bind=True,
    queue='test_queue',
    name='scrape_car_test',
)
def scrape_car_test(
    self,
    run_id,
    car_brand: Optional[str] = None,
    car_model: Optional[str] = None,
    fuel_type: Optional[str] = None,
    variant: Optional[str] = None,
    year: Optional[str] = None,
    rto_code: Optional[str] = None,
    ncb_percent: Optional[str] = None,
    cust_name: Optional[str] = None,
    phone: Optional[str] = None,
    policy_expiry: Optional[str] = None,
    claim_status: Optional[str] = None,
    quotes_url: Optional[str] = None,
    user_profile_dir: Optional[str] = None,
    profile_unique_key: Optional[str] = None,
    profile_identifier_key: Optional[str] = None,
):
    """Dummy stand-in for scrape_car — logs received kwargs and returns immediately.
    Runs on 'test_queue' so it never competes with real scrape_car work on 'celery'."""
    car_label = f"{car_brand or ''} {car_model or ''} {variant or ''}".strip() or run_id
    logger.info(f"[TEST] scrape_car_test received run_id={run_id} car={car_label} "
                f"rto={rto_code} ncb={ncb_percent} profile_unique_key={profile_unique_key} "
                f"profile_identifier_key={profile_identifier_key}")
    return {
        "status": "TEST_OK",
        "run_id": run_id,
        "car_label": car_label,
        "profile_unique_key": profile_unique_key,
        "profile_identifier_key": profile_identifier_key,
    }


@app.task(bind=True, base=CallbackTask)
def health_check(self):
    """Health check task"""
    logger.info("🏥 Health check OK")
    return {"status": "HEALTHY"}


@app.task(bind=True, name="pipeline_catchup", ignore_result=True)
def pipeline_catchup(self):
    """
    Periodic safety-net: find every SUCCESS run that has no final_flat_output
    entry and run the pipeline for it. Fires every 5 minutes via Celery beat.
    Complements the per-scrape auto-trigger in db_live_sync._auto_run_pipeline().
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            SELECT run_id::text FROM scrape_runs
            WHERE status = 'SUCCESS'
              AND run_id::text NOT IN (
                  SELECT run_id::text FROM final_flat_output
              )
        """)
        missing = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()

        if not missing:
            logger.info("✅ Pipeline catchup: all SUCCESS runs already processed")
            return

        logger.info(f"🔄 Pipeline catchup: {len(missing)} unprocessed run(s) found")

        from RSscraping_backend.backend.db_complete_flow.db_pipeline import run_pipeline_v2

        ok = fail = 0
        for run_id in missing:
            try:
                success = run_pipeline_v2.run_pipeline(run_id)
                if success:
                    ok += 1
                    logger.info(f"✅ Catchup pipeline done: run_id={run_id}")
                else:
                    fail += 1
                    logger.warning(f"⚠️  Catchup pipeline skipped (no data): run_id={run_id}")
            except Exception as e:
                fail += 1
                logger.error(f"❌ Catchup pipeline failed for run_id={run_id}: {e}")

        logger.info(f"📊 Pipeline catchup complete — ok={ok}, failed={fail}")

    except Exception as e:
        logger.error(f"❌ Pipeline catchup task error: {e}")


if __name__ == "__main__":
    app.start()
