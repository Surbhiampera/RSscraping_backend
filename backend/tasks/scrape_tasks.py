# The real scrape_car task is registered in backend/celery/task_queue/tasks.py.
# That module is loaded by celery_worker.py and is the single source of truth
# for the "scrape_car" task name on the Celery worker.
#
# Do NOT register another task with name="scrape_car" here — duplicate
# registrations cause the worker to pick up whichever was imported last,
# silently overriding the real implementation.
#
# To call the task from Python code, use task_sender.send_scrape_row() or
# task_sender.send_scrape_car() which call celery_app.send_task("scrape_car").

from backend.celery.task_queue.tasks import scrape_car  # noqa: F401  (re-export for convenience)