#!/bin/bash
# Start Celery worker for the task_queue module.
# Run from the RSscraping_backend/ directory:
#   bash task_queue/start_worker.sh
#
# Environment variables:
#   CONCURRENCY   Number of worker processes (default: 1)
#   LOGLEVEL      Celery log level (default: info)

set -e

# Move to RSscraping_backend/ regardless of where the script is called from
cd "$(dirname "$0")/.."

# Activate virtualenv if present
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
elif [ -f "../venv/bin/activate" ]; then
    source ../venv/bin/activate
fi

echo "==========================================="
echo " Starting Celery worker — task_queue.task"
echo " Concurrency : ${CONCURRENCY:-1}"
echo " Log level   : ${LOGLEVEL:-info}"
echo "==========================================="

celery -A task_queue.task worker \
    --queues=celery \
    --concurrency="${CONCURRENCY:-1}" \
    --loglevel="${LOGLEVEL:-info}" \
    --hostname="worker@%h"
