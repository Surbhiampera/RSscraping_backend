"""
Azure Redis Queue — direct RPUSH for all 4 scrape flows.

Produces plain-JSON messages so any consumer can BLPOP without Celery.
Connection reuses the same Azure Cache for Redis used as the Celery broker.

Flows pushed here:
  with_reg   + upload  (Flow 1)
  without_reg + upload  (Flow 2)
  with_reg   + manual  (Flow 3)
  without_reg + manual  (Flow 4)
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

import redis

from backend.core.config import settings

logger = logging.getLogger(__name__)


def _get_client() -> redis.Redis:
    return redis.Redis(
        host=settings.REDIS_HOST,
        port=6380,
        password=settings.REDIS_KEY,
        ssl=True,
        ssl_cert_reqs=None,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )


def push_scrape_task(
    run_id: str,
    car_number: str,
    input_data: Optional[str] = None,
    cust_name: Optional[str] = None,
    phone: Optional[str] = None,
    policy_expiry: Optional[str] = None,
    claim_status: Optional[str] = None,
) -> bool:
    """
    Push one scrape task to the Azure Redis queue.

    flow is inferred automatically:
      input_data is None  → with_reg  (flows 1 & 3)
      input_data not None → without_reg (flows 2 & 4)
    """
    flow = "without_reg" if input_data else "with_reg"

    message = {
        "run_id": run_id,
        "car_number": car_number,
        "flow": flow,
        "input_data": json.loads(input_data) if input_data else None,
        "cust_name": cust_name,
        "phone": phone,
        "policy_expiry": policy_expiry,
        "claim_status": claim_status,
        "queued_at": datetime.now(timezone.utc).isoformat(),
    }

    queue_name = settings.AZURE_QUEUE_NAME

    try:
        client = _get_client()
        client.rpush(queue_name, json.dumps(message))
        logger.info(
            "📤 Azure Redis queue [%s] ← %s | run_id=%s | flow=%s",
            queue_name, car_number, run_id, flow,
        )
        return True
    except Exception as exc:
        logger.error("❌ Azure Redis push failed for %s: %s", car_number, exc)
        return False
