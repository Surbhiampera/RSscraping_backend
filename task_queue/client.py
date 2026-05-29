#!/usr/bin/env python
"""
Celery Task Client - Submit and monitor scraping tasks
"""

import sys
import logging
from typing import List, Optional
from celery.result import AsyncResult
from task_queue.task import app, scrape_car, health_check

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def submit_task(
    run_id: str,
    car_brand: str,
    car_model: str,
    fuel_type: str,
    variant: str,
    year: str,
    rto_code: str,
    cust_name: Optional[str] = None,
    phone: Optional[str] = None,
    policy_expiry: Optional[str] = None,
    claim_status: Optional[str] = None,
) -> Optional[str]:
    """Submit a single scraping task to the Celery queue."""
    try:
        label = f"{car_brand} {car_model} {variant} ({rto_code})"
        logger.info(f"📤 Submitting: {label}")
        result = scrape_car.delay(
            run_id=run_id,
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
        )
        logger.info(f"✅ Submitted task_id={result.id}")
        return result.id
    except Exception as e:
        logger.error(f"❌ Submit failed: {e}")
        return None


def check_status(task_id: str):
    """Check task status"""
    result = AsyncResult(task_id, app=app)
    print(f"\n📊 Task: {task_id}")
    print(f"   State: {result.state}")
    
    if result.state == "SUCCESS":
        print(f"   Result: {result.result}")
    elif result.state in ["RETRY", "FAILURE"]:
        print(f"   Error: {result.info}")
    else:
        print(f"   Info: {result.info}")
    
    return result.state


def check_batch(task_ids: List[str]):
    """Check multiple task statuses"""
    print(f"\n📊 Batch Status ({len(task_ids)} tasks):\n")
    
    completed = 0
    failed = 0
    pending = 0
    
    for task_id in task_ids:
        result = AsyncResult(task_id, app=app)
        state = result.state
        
        if state == "SUCCESS":
            completed += 1
            status = "✅"
        elif state in ["FAILURE", "RETRY"]:
            failed += 1
            status = "❌"
        else:
            pending += 1
            status = "⏳"
        
        print(f"  {status} {task_id[:16]}... → {state}")
    
    print(f"\n📈 Summary:")
    print(f"   Completed: {completed}")
    print(f"   Failed: {failed}")
    print(f"   Pending: {pending}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python client.py status TASK_ID")
        print("  python client.py check TASK_ID1 TASK_ID2 ...")
        print("  python client.py health")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "status" and len(sys.argv) > 2:
        check_status(sys.argv[2])

    elif cmd == "check" and len(sys.argv) > 2:
        check_batch(sys.argv[2:])

    elif cmd == "health":
        print("🏥 Running health check...")
        result = health_check.delay()
        try:
            res = result.get(timeout=5)
            print(f"✅ {res}")
        except Exception as e:
            print(f"❌ {e}")

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
