""" Schedules module
"""
from typing import List, Optional, Union
import asyncio
from ipcalc import Network
from aioping import ping
from aioredis import Redis
from redisqueue import AioRedisQueue


async def save_hosts(redis: Redis,
                     hosts: List[str],
                     expire: Optional[int] = 300) -> None:
    """ Save hosts to DB
    """
    pipe = redis.pipeline()

    for host in hosts:
        pipe.set(f'host:{host}', host, expire=expire)

    await pipe.execute()


async def load_hosts(redis: Redis) -> List[str]:
    """ Load hosts from DB
    """
    keys = await redis.keys('host:*')
    pipe = redis.pipeline()

    for key in keys:
        pipe.get(key, encoding='utf-8')

    return await pipe.execute()


async def find_hosts(redis: Redis, network: str) -> None:
    """ Scan the network to detect live hosts
    """
    async def is_alive(host: str) -> Union[None, str]:
        try:
            await ping(dest_addr=host, timeout=10)
        except TimeoutError:
            return None
        else:
            return host

    tasks: List = []
    for host in Network(network):
        tasks.append(asyncio.ensure_future(is_alive(str(host))))

    hosts = await asyncio.gather(*tasks)
    hosts = list(filter(None, hosts))

    await save_hosts(redis, hosts, expire=900)


async def check_icmp(queue: AioRedisQueue) -> None:
    """ ICMP queue seed
    """
    for address in await load_hosts(redis=queue.redis):
        await queue.put({
            'series': 'icmp',
            'address': address,
            'count': 5,
            'timeout': 3
        })


async def check_system(queue: AioRedisQueue,
                       community: Optional[str] = 'public') -> None:
    """ SNMP queue seed
    """
    for address in await load_hosts(redis=queue.redis):
        await queue.put({
            'series': 'system',
            'address': address,
            'community': community,
            'oids': {
                'uptime': '.1.3.6.1.2.1.1.3'
            }
        })


async def check_interfaces(queue: AioRedisQueue,
                           community: Optional[str] = 'public') -> None:
    """ SNMP queue seed
    """
    for address in await load_hosts(redis=queue.redis):
        await queue.put({
            'series': 'interfaces',
            'tags': ['type', 'name', 'mac'],
            'address': address,
            'community': community,
            'oids': {
                'type': '.1.3.6.1.2.1.2.2.1.3',
                'mac': '.1.3.6.1.2.1.2.2.1.6',
                'admin_status': '.1.3.6.1.2.1.2.2.1.7',
                'oper_status': '.1.3.6.1.2.1.2.2.1.8',
                'in_discards': '.1.3.6.1.2.1.2.2.1.13',
                'in_errors': '.1.3.6.1.2.1.2.2.1.14',
                'out_discards': '.1.3.6.1.2.1.2.2.1.19',
                'out_errors': '.1.3.6.1.2.1.2.2.1.20',
                'name': '.1.3.6.1.2.1.31.1.1.1.1',
                'in_octets': '.1.3.6.1.2.1.31.1.1.1.6',
                'out_octets': '.1.3.6.1.2.1.31.1.1.1.10'
            }
        })
