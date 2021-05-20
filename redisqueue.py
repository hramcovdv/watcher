""" RedisQueue module
"""
from typing import Any, Text, Dict, Optional
import json
from aioredis import Redis


class AioRedisQueue():
    """ AioRedisQueue class
    """
    def __init__(self,
                 redis: Redis,
                 queue_name: Optional[str] = 'queue') -> None:
        """ Create a queue.
        """
        self.redis = redis
        self.name = queue_name

    async def put(self, item: Dict[Text, Any]) -> Any:
        """ Inserts item into the queue.
        """
        return await self.redis.rpush(self.name, json.dumps(item))

    async def get(self,
                  block: Optional[bool] = True,
                  timeout: Optional[int] = 0) -> Any:
        """ Get item from the queue.
        """
        if block:
            item = await self.redis.blpop(self.name, timeout=timeout)
        else:
            item = await self.redis.lpop(self.name)

        return item if item is None else json.loads(item[1])

    async def clear(self) -> Any:
        """ Clear all items from queue.
        """
        return await self.redis.ltrim(self.name, 1, 0)

    async def size(self) -> Any:
        """ Return size of the queue.
        """
        return await self.redis.llen(self.name)
