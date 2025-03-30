from celery import Celery
from settings import settings

celery_app = Celery(
    "tasks",
    broker=settings.redis.url,
    backend=settings.redis.url,
)