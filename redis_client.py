import redis
from settings import settings

redis_client = redis.Redis(
    host= settings.redis.host,
    password=settings.redis.password,
    decode_responses=True
)
