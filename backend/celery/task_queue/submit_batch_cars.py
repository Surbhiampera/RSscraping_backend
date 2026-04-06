#!/usr/bin/env python
"""
Submit 4 car scraping tasks to Celery queue for parallel processing
Architecture: Client → Celery → Redis Queue → Workers → pb_flow.py → DB
"""

import logging
from datetime import datetime
from task_queue.client import submit_task

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# CARS TO SCRAPE
# ============================================================================
CARS_TO_SCRAPE = [
    "MH12VZ2302",
    "MH49BB1307",
    "MH12SE5466",
    "MH04KW1827",
]


def submit_batch_cars():
    """Submit 4 car scraping tasks to Celery queue"""
    
    print("\n" + "="*80)
    print("🚀 SUBMITTING 4-CAR BATCH TO CELERY QUEUE")
    print("="*80)
    print(f"📋 Cars: {', '.join(CARS_TO_SCRAPE)}")
    print(f"⏰ Submitted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")
    
    submitted_tasks = []
    task_ids = []
    
    for car_number in CARS_TO_SCRAPE:
        print(f"\n📤 Submitting: {car_number}")
        
        try:
            task_id = submit_task(
                car_number=car_number,
                phone=None,
                cust_name=None,
                policy_expiry="Policy not expired yet",
                claim_status="Not Sure",
            )
            
            if task_id:
                submitted_tasks.append({"status": "SUBMITTED", "task_id": task_id, "car": car_number})
                task_ids.append(task_id)
                print(f"   ✅ Task ID: {task_id}")
            else:
                submitted_tasks.append({"status": "FAILED", "car": car_number})
                print(f"   ❌ Failed to submit")
        
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            submitted_tasks.append({"status": "FAILED", "car": car_number, "error": str(e)})
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    successful = sum(1 for t in submitted_tasks if t['status'] == 'SUBMITTED')
    failed = sum(1 for t in submitted_tasks if t['status'] != 'SUBMITTED')
    
    print("\n" + "="*80)
    print("📊 BATCH SUBMISSION SUMMARY")
    print("="*80)
    print(f"Total: {len(CARS_TO_SCRAPE)}")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print("="*80 + "\n")
    
    # =========================================================================
    # TASK IDs
    # =========================================================================
    if task_ids:
        print("📌 Task IDs for monitoring:\n")
        for car, task_id in zip(CARS_TO_SCRAPE, task_ids):
            print(f"   {car:15} → {task_id}")
    
    print("\n" + "="*80)
    print("⏭️  NEXT STEPS")
    print("="*80)
    print(f"""
1️⃣  Monitor tasks:
   python -m task_queue.client check {' '.join(task_ids[:2])}...

2️⃣  Check database:
   SELECT * FROM scrape_runs WHERE created_at > NOW() - INTERVAL '1 hour'

3️⃣  View data:
   ls policy_bazaar_responses_validation/
""")
    print("="*80 + "\n")
    
    return submitted_tasks, task_ids


if __name__ == "__main__":
    try:
        tasks, ids = submit_batch_cars()
        if ids:
            print(f"✅ Submitted {len(ids)} tasks")
        else:
            print("⚠️  No tasks were submitted")
    except KeyboardInterrupt:
        print("\n⚠️  Cancelled")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
