import os
from celery import Celery

redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery(
    "bank_statement_api",
    broker=redis_url,
    backend=redis_url,
    include=["app.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    worker_concurrency=int(os.getenv("CELERY_CONCURRENCY", "2")),
    worker_prefetch_multiplier=1,
)
