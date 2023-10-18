import redis.asyncio as aioredis
from channels_redis.pubsub import RedisPubSubChannelLayer


def get_default_redis_client():
    return aioredis.Redis.from_url("redis://localhost", decode_responses=False)


def get_default_channel_layer():
    return RedisPubSubChannelLayer(
        hosts=["redis://localhost"],
        expiry=60 * 60 * 2,
        group_expiry=60 * 60 * 2,
        capacity=1000,
    )


class QueueLayer:
    def __init__(self, redis_client: aioredis.Redis):
        self._redis_client = redis_client

    async def rpush(self, queue_name: str, event: bytes):
        await self._redis_client.rpush(queue_name, event)

    async def blpop(self, queue_name: str, timeout: int):
        _, message = await self._redis_client.blpop([queue_name], timeout=timeout)
        return message
