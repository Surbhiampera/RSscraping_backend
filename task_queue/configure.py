"""
Celery + Redis configuration for the task_queue module.
All task queue settings are centralised here.
"""

from backend.core.config import settings

# ── Redis ────────────────────────────────────────────────────────────────────
REDIS_URL = settings.REDIS_URL

# ── Celery broker / backend ──────────────────────────────────────────────────
CELERY_BROKER_URL     = REDIS_URL
CELERY_RESULT_BACKEND = REDIS_URL

# ── Serialisation ────────────────────────────────────────────────────────────
CELERY_TASK_SERIALIZER   = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_ACCEPT_CONTENT    = ["json"]

# ── Timezone ─────────────────────────────────────────────────────────────────
CELERY_TIMEZONE   = "Asia/Kolkata"
CELERY_ENABLE_UTC = True

# ── Task limits ──────────────────────────────────────────────────────────────
CELERY_TASK_TIME_LIMIT          = 60 * 60   # 60 minutes hard limit
CELERY_TASK_SOFT_TIME_LIMIT     = 40 * 60   # 40 minutes soft limit
CELERY_TASK_MAX_RETRIES         = 2
CELERY_TASK_DEFAULT_RETRY_DELAY = 60        # seconds between retries

# ── Result expiry ────────────────────────────────────────────────────────────
CELERY_RESULT_EXPIRES    = 24 * 60 * 60     # 24 hours
CELERY_RESULT_PERSISTENT = True

# ── Database ─────────────────────────────────────────────────────────────────
DB_CONFIG = settings.DB_CONFIG

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_LEVEL = "INFO"
