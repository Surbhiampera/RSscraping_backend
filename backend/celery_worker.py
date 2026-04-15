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

CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TIMEZONE = "Asia/Kolkata"
CELERY_ENABLE_UTC = True
 
CELERY_TASK_TIME_LIMIT = 60 * 60  # 60 minutes
CELERY_TASK_SOFT_TIME_LIMIT = 40 * 60  # 40 minutes
CELERY_TASK_MAX_RETRIES = 3
CELERY_TASK_DEFAULT_RETRY_DELAY = 60
 
CELERY_RESULT_EXPIRES = 24 * 60 * 60  # 24 hours
CELERY_RESULT_PERSISTENT = True
 
# =======================================
 
 
celery_app.conf.update(
    task_serializer=CELERY_TASK_SERIALIZER,
    result_serializer=CELERY_RESULT_SERIALIZER,
    accept_content=CELERY_ACCEPT_CONTENT,
    timezone=CELERY_TIMEZONE,
    enable_utc=CELERY_ENABLE_UTC,
    task_time_limit=CELERY_TASK_TIME_LIMIT,
    task_soft_time_limit=CELERY_TASK_SOFT_TIME_LIMIT,
    result_expires=24 * 60 * 60,
    task_track_started=True,
    broker_use_ssl={"ssl_cert_reqs": ssl.CERT_NONE},
    redis_backend_use_ssl={"ssl_cert_reqs": ssl.CERT_NONE},
)
 
 
