"""
Celery app for the FastAPI side.

Imports the single Celery app from tasks.py so all tasks are registered
on the same instance used by the worker.
"""

from backend.celery.task_queue.tasks import app as celery_app  # noqa: F401
