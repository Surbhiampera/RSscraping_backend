"""
Celery + Redis Configuration - All task queue settings centralized here
"""

from backend.core.config import settings

# ============================================================================
# REDIS CONFIGURATION
# ============================================================================
REDIS_URL = settings.REDIS_URL

# ============================================================================
# CELERY CONFIGURATION
# ============================================================================
CELERY_BROKER_URL = REDIS_URL
CELERY_RESULT_BACKEND = REDIS_URL


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

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================
DB_CONFIG = settings.DB_CONFIG

# ============================================================================
# LOGGING
# ============================================================================
LOG_LEVEL = "INFO"
