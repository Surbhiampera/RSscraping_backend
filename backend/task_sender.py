#!/usr/bin/env python
"""
Task Sender — single file for all Celery task dispatching.

Used by:
- FastAPI (import)
- CLI (python -m backend.task_sender)

Only responsibility:
➡️ Send tasks to Celery (NO DB logic here)
"""

import sys
import logging
from typing import List, Optional, Dict, Any

from celery.result import AsyncResult
from backend.celery_worker import celery_app

# ── Logging ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ───────────────────────────────────────────────────────────────────────────
# ✅ CORE FUNCTION (USED BY SERVICE — MOST IMPORTANT)
# ───────────────────────────────────────────────────────────────────────────

def send_scrape_row(run_id: str, inp) -> Optional[str]:
    """
    Send a single DB row as a Celery task.

    Args:
        run_id: ScrapeRun ID
        inp:    ScrapeRunInput ORM object

    Returns:
        task_id (str) or None
    """
    try:
        result = celery_app.send_task(
            "scrape_car",
            kwargs=dict(
                car_number=inp.car_number,
                run_id=run_id,
                cust_name=getattr(inp, "customer_name", None),
                phone=getattr(inp, "phone", None),
                policy_expiry=getattr(inp, "policy_expiry", None),
                claim_status=getattr(inp, "claim_status", None),
            ),
        )

        logger.info(f"📤 Sent scrape_car → {inp.car_number} | task_id={result.id}")
        return result.id

    except Exception as e:
        logger.error(f"❌ Failed for {inp.car_number}: {e}")
        return None


# ───────────────────────────────────────────────────────────────────────────
# ⚠️ OPTIONAL (ONLY IF YOU STILL NEED BULK DICT SUPPORT)
# ───────────────────────────────────────────────────────────────────────────

def send_scrape_run(run_id: str, cars: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """
    Send one task per car (dict-based input).
    ⚠️ Not recommended for large files (use send_scrape_row instead)
    """
    results = []

    for car in cars:
        try:
            result = celery_app.send_task(
                "scrape_car",
                kwargs=dict(
                    car_number=car.get("car_number"),
                    run_id=run_id,
                    cust_name=car.get("customer_name"),
                    phone=car.get("phone"),
                    policy_expiry=car.get("policy_expiry"),
                    claim_status=car.get("claim_status"),
                ),
            )

            results.append({
                "car_number": car.get("car_number"),
                "task_id": result.id
            })

            logger.info(f"📤 Sent → {car.get('car_number')} | task_id={result.id}")

        except Exception as e:
            logger.error(f"❌ Failed for {car.get('car_number')}: {e}")

    logger.info(f"✅ Queued {len(results)}/{len(cars)} tasks for run {run_id}")
    return results


# ───────────────────────────────────────────────────────────────────────────
# 🔹 SINGLE CAR (CLI / DEBUG USE)
# ───────────────────────────────────────────────────────────────────────────

def send_scrape_car(
    car_number: str,
    run_id: Optional[str] = None,
    phone: Optional[str] = None,
    cust_name: Optional[str] = None,
    policy_expiry: Optional[str] = None,
    claim_status: Optional[str] = None,
) -> Optional[str]:
    """Send a single car scrape task."""
    try:
        result = celery_app.send_task(
            "scrape_car",
            kwargs=dict(
                car_number=car_number,
                run_id=run_id,
                phone=phone,
                cust_name=cust_name,
                policy_expiry=policy_expiry,
                claim_status=claim_status,
            ),
        )

        logger.info(f"📤 Sent scrape_car → {car_number} | task_id={result.id}")
        return result.id

    except Exception as e:
        logger.error(f"❌ Failed to send scrape_car: {e}")
        return None


# ───────────────────────────────────────────────────────────────────────────
# 🔹 BATCH (CLI ONLY)
# ───────────────────────────────────────────────────────────────────────────

def send_batch(cars: List[str]) -> List[str]:
    logger.info(f"📦 Sending {len(cars)} cars")

    task_ids = []
    for car in cars:
        task_id = send_scrape_car(car_number=car)
        if task_id:
            task_ids.append(task_id)

    logger.info(f"✅ Sent {len(task_ids)}/{len(cars)} tasks")
    return task_ids


# ───────────────────────────────────────────────────────────────────────────
# 🔹 TASK STATUS
# ───────────────────────────────────────────────────────────────────────────

def get_task_status(task_id: str) -> dict:
    result = AsyncResult(task_id, app=celery_app)

    return {
        "task_id": task_id,
        "state": result.state,
        "ready": result.ready(),
        "successful": result.successful() if result.ready() else None,
        "result": result.result if result.ready() else None,
    }


# ───────────────────────────────────────────────────────────────────────────
# 🔹 CLI ENTRY
# ───────────────────────────────────────────────────────────────────────────

def main():
    """CLI usage"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m backend.task_sender send  CAR_NUMBER")
        print("  python -m backend.task_sender batch CAR1 CAR2")
        print("  python -m backend.task_sender status TASK_ID")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "send" and len(sys.argv) > 2:
        task_id = send_scrape_car(car_number=sys.argv[2])
        if task_id:
            print(f"\n✅ Task: {task_id}")

    elif cmd == "batch" and len(sys.argv) > 2:
        cars = sys.argv[2:]
        task_ids = send_batch(cars)

        print(f"\n✅ Submitted {len(task_ids)} tasks:")
        for car, tid in zip(cars, task_ids):
            print(f"   {car} → {tid}")

    elif cmd == "status" and len(sys.argv) > 2:
        status = get_task_status(sys.argv[2])

        print(f"\n📊 Task: {status['task_id']}")
        print(f"   State: {status['state']}")
        if status["result"]:
            print(f"   Result: {status['result']}")

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()