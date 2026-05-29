#!/usr/bin/env python
"""
Submit car scraping tasks to Celery queue for parallel processing
Architecture: Client → Celery → Redis Queue → Workers → cmf_locator_v2.py → DB
"""

import sys
from pathlib import Path
import logging
from typing import Optional
import uuid

sys.path.insert(0, str(Path(__file__).parent.parent))

from celery.result import AsyncResult
from task_queue.task import app, scrape_car

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ── Test car ──────────────────────────────────────────────────────────────────
TEST_CAR_BRAND = "Maruti"
TEST_CAR_MODEL = "Alto"
TEST_FUEL_TYPE = "Petrol"
TEST_VARIANT   = "LXI"
TEST_YEAR      = "2018"
TEST_RTO_CODE  = "TS11"
# ─────────────────────────────────────────────────────────────────────────────


def send_task(
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
    """Send a single scrape task to the Celery queue."""
    try:
        run_id = str(uuid.uuid4())
        label = f"{car_brand} {car_model} {variant} ({rto_code})"
        logger.info(f"Sending: {label}")
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
        task_id = result.id
        logger.info(f"Task sent — run_id={run_id} | task_id={task_id}")
        return task_id
    except Exception as e:
        logger.exception(f"Error sending task: {e}")
        return None


def get_task_status(task_id: str) -> dict:
    """Get task status from Redis."""
    result = AsyncResult(task_id, app=app)
    return {
        "task_id": task_id,
        "state": result.state,
        "ready": result.ready(),
        "successful": result.successful() if result.ready() else None,
        "result": result.result if result.ready() else None,
    }


def main():
    """CLI interface"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python submit_batch_cars.py test")
        print("  python submit_batch_cars.py status TASK_ID")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "test":
        task_id = send_task(
            car_brand=TEST_CAR_BRAND,
            car_model=TEST_CAR_MODEL,
            fuel_type=TEST_FUEL_TYPE,
            variant=TEST_VARIANT,
            year=TEST_YEAR,
            rto_code=TEST_RTO_CODE,
            policy_expiry="Policy not expired yet",
            claim_status="Not Sure",
        )
        if task_id:
            print(f"\nTask submitted: {task_id}")

    elif cmd == "status" and len(sys.argv) > 2:
        status = get_task_status(sys.argv[2])
        print(f"\nTask:   {status['task_id']}")
        print(f"State:  {status['state']}")
        if status['result']:
            print(f"Result: {status['result']}")

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
