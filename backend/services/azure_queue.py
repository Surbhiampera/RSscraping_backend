"""
Azure Redis Queue — RPUSH in Celery wire format.

Produces messages identical to scrape_car.delay() so any Celery worker
can BLPOP from this queue and execute the task directly.

Flows pushed here:
  with_reg   + upload  (Flow 1)
  without_reg + upload  (Flow 2)
  with_reg   + manual  (Flow 3)
  without_reg + manual  (Flow 4)
"""

import base64
import json
import logging
import socket
import uuid
from typing import Optional

import redis

from backend.core.config import settings

logger = logging.getLogger(__name__)

_HOSTNAME = socket.gethostname()


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


def _build_celery_message(
    task_id: str,
    queue_name: str,
    run_id: str,
    car_brand: Optional[str],
    car_model: Optional[str],
    fuel_type: Optional[str],
    variant: Optional[str],
    year: Optional[str],
    rto_code: Optional[str],
    ncb_percent: Optional[str],
    cust_name: Optional[str],
    phone: Optional[str],
    policy_expiry: Optional[str],
    claim_status: Optional[str],
) -> str:
    """Build a Celery wire-format message and return it as a JSON string."""
    kwargs = {
        "run_id":        run_id,
        "car_brand":     car_brand,
        "car_model":     car_model,
        "fuel_type":     fuel_type,
        "variant":       variant,
        "year":          year,
        "rto_code":      rto_code,
        "ncb_percent":   ncb_percent,
        "cust_name":     cust_name,
        "phone":         phone,
        "policy_expiry": policy_expiry,
        "claim_status":  claim_status,
    }

    body_bytes = json.dumps(
        [[], kwargs, {"callbacks": None, "errbacks": None, "chain": None, "chord": None}]
    ).encode("utf-8")
    body_b64 = base64.b64encode(body_bytes).decode("utf-8")

    message = {
        "body": body_b64,
        "content-encoding": "utf-8",
        "content-type": "application/json",
        "headers": {
            "lang": "py",
            "task": "scrape_car",
            "id": task_id,
            "shadow": None,
            "eta": None,
            "expires": None,
            "group": None,
            "group_index": None,
            "retries": 0,
            "timelimit": [None, None],
            "root_id": task_id,
            "parent_id": None,
            "argsrepr": "()",
            "kwargsrepr": repr(kwargs),
            "origin": f"gen@{_HOSTNAME}",
            "ignore_result": False,
            "replaced_task_nesting": 0,
            "stamped_headers": None,
            "stamps": {},
        },
        "properties": {
            "correlation_id": task_id,
            "reply_to": str(uuid.uuid4()).replace("-", "") + "@/",
            "delivery_mode": 2,
            "delivery_info": {"exchange": "", "routing_key": queue_name},
            "priority": 0,
            "body_encoding": "base64",
            "delivery_tag": str(uuid.uuid4()),
        },
    }
    return json.dumps(message)


def push_scrape_task(
    run_id: str,
    car_brand: Optional[str] = None,
    car_model: Optional[str] = None,
    fuel_type: Optional[str] = None,
    variant: Optional[str] = None,
    year: Optional[str] = None,
    rto_code: Optional[str] = None,
    ncb_percent: Optional[str] = None,
    car_number: Optional[str] = None,
    cust_name: Optional[str] = None,
    phone: Optional[str] = None,
    policy_expiry: Optional[str] = None,
    claim_status: Optional[str] = None,
) -> bool:
    """
    Push one scrape task to the Azure Redis queue in Celery wire format.

    flow is inferred automatically:
      car_number set, no structured fields → with_reg  (flows 1 & 3)
      structured fields set               → without_reg (flows 2 & 4)
    """
    car_label  = f"{car_brand or ''} {car_model or ''} {variant or ''}".strip() or car_number or run_id
    task_id    = str(uuid.uuid4())
    queue_name = settings.AZURE_QUEUE_NAME

    try:
        message_str = _build_celery_message(
            task_id=task_id,
            queue_name=queue_name,
            run_id=run_id,
            car_brand=car_brand,
            car_model=car_model,
            fuel_type=fuel_type,
            variant=variant,
            year=year,
            rto_code=rto_code,
            ncb_percent=ncb_percent,
            cust_name=cust_name,
            phone=phone,
            policy_expiry=policy_expiry,
            claim_status=claim_status,
        )
        client = _get_client()
        client.rpush(queue_name, message_str)
        logger.info(
            "📤 Azure Redis queue [%s] ← %s | run_id=%s | task_id=%s",
            queue_name, car_label, run_id, task_id,
        )
        return True
    except Exception as exc:
        logger.error("❌ Azure Redis push failed for %s: %s", car_label, exc)
        return False
