"""
Celery app configuration for the FastAPI side.

Creates a Celery instance connected to the same broker as the worker.
All task sending logic lives in backend/task_sender.py.
Task execution logic lives in backend/celery/task_queue/tasks.py.
"""

from celery import Celery
from backend.core.config import settings
import ssl

celery_app = Celery(
    "insurance_scraper",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Kolkata",
    enable_utc=True,
    broker_use_ssl={"ssl_cert_reqs": ssl.CERT_NONE},
    redis_backend_use_ssl={"ssl_cert_reqs": ssl.CERT_NONE},
)
