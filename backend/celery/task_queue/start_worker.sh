#!/bin/bash
# Start Celery Worker

cd "$(dirname "$0")/.."

if [ -d "venv" ]; then
    source venv/bin/activate
fi

echo "Starting Celery Worker..."
celery -A task_queue.tasks worker \
    --queues=scraper \
    --concurrency=${CONCURRENCY:-1} \
    --loglevel=info \
    --hostname=worker@%h
