"""
Celery Tasks with Direct Database Integration
Automatically saves scraped responses to PostgreSQL on completion
"""

import asyncio
import logging
import psycopg2
import json
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
from celery import Celery, Task
from celery.exceptions import SoftTimeLimitExceeded

from task_queue.config import (
    CELERY_BROKER_URL,
    CELERY_RESULT_BACKEND,
    CELERY_TASK_TIME_LIMIT,
    CELERY_TASK_SOFT_TIME_LIMIT,
    CELERY_TASK_MAX_RETRIES,
    CELERY_TASK_DEFAULT_RETRY_DELAY,
    DB_CONFIG,
    LOG_LEVEL,
)

# Database sync imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from db_and_logging.db_live_sync import LiveDBSync

# ============================================================================
# CELERY APP
# ============================================================================
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
    car_number: str,
    cust_name: Optional[str] = None,
    phone: Optional[str] = None,
    policy_expiry: Optional[str] = None,
    claim_status: Optional[str] = None,
    user_profile_dir: Optional[str] = None,
):
    """
    Scrape insurance quotes for a car and automatically save to database
    
    Args:
        car_number: Car registration number
        phone: Customer phone (optional)
        cust_name: Customer name (optional)
        policy_expiry: Policy expiry status (optional)
        claim_status: Claim status (optional)
    
    Returns:
        dict: Task result with DB insertion status
    """
    
    logger.info(f"\n{'='*70}")
    logger.info(f" TASK STARTED: {run_id}")
    logger.info(f" Car: {car_number}")
    logger.info(f"{'='*70}")

    
    try:
        import time
        start_time = time.time()
        
        # Add paths
        project_root = Path(__file__).parent.parent
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        pb_scripts_path = str(project_root / "pb_scraper")
        if pb_scripts_path not in sys.path:
            sys.path.insert(0, pb_scripts_path)
        
        try:
            from pb_scraper.cmf_locator_v2 import run as run_scraper
        except ImportError:
            logger.error(f"❌ Could not import pb_flow from {pb_scripts_path}")
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

        print( f"Phone type: {type(phone)}" )
        print( f"Phone value: {phone}" )
        
        asyncio.run(
            run_scraper(
                run_id=run_id,
                car_number=car_number,
                car_name="NEW_CAR",
                cust_name=cust_name,
                phone=phone,
                policy_expiry=policy_expiry,
                claim_status=claim_status,
                user_profile_dir = user_profile_dir
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
            status="SUCCESS",
            total_duration_ms=total_duration_ms,
            notes=f"Car: {car_number} | Phone: {phone or 'N/A'}"
        )
        
        result = {
            "status": "SUCCESS",
            "car_number": car_number,
            "task_id": run_id,
            "duration_ms": total_duration_ms,
        }
        
        logger.info(f"\n{'='*70}")
        logger.info(f"✅ TASK COMPLETED: {run_id}")
        logger.info(f"⏱️ Duration: {total_duration_ms}ms")
        logger.info(f"{'='*70}\n")
        
        return result
    
    except SoftTimeLimitExceeded:
        logger.warning(f"⏱️  Soft timeout for {car_number}")
        insert_scrape_run_to_db(run_id, car_number, "TIMEOUT", "Soft time limit exceeded")
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


@app.task(bind=True, base=CallbackTask)
def health_check(self):
    """Health check task"""
    logger.info("🏥 Health check OK")
    return {"status": "HEALTHY"}


if __name__ == "__main__":
    app.start()
