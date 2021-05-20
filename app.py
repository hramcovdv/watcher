""" Worker manager.

Usage:
  app.py [options] [--tasks=COUNT]
  app.py -h | --help
  app.py --version

Options:
  -h --help        Show this screen.
  --version        Show version.
  --tasks=COUNT    Set count of tasks [default: 10].
  --log-file=FILE  Set name of logging file."""
import os
import sys
import logging
import asyncio
from typing import Any, Dict
from docopt import docopt
from dotenv import dotenv_values
from aioinflux import InfluxDBClient
from aioredis import create_redis_pool
from redisqueue import AioRedisQueue
from workers import icmp_worker, snmp_worker, influx_callback

DEFAULT_CONFIG = {
    'REDIS_HOST': 'localhost',
    'REDIS_PORT': 6379,
    'REDIS_DB': 0,
    'INFLUXDB_HOST': 'localhost',
    'INFLUXDB_PORT': 8086,
    'INFLUXDB_USERNAME': '',
    'INFLUXDB_PASSWORD': '',
    'INFLUXDB_DB': 'default',
    'ICMP_QUEUE_NAME': 'queue:icmp',
    'SNMP_QUEUE_NAME': 'queue:snmp',
    'TASKS': 10
}


async def main(config: Dict[str, Any]) -> None:
    """ Main worker
    """
    async with InfluxDBClient(host=config['INFLUXDB_HOST'],
                              port=int(config['INFLUXDB_PORT']),
                              username=config['INFLUXDB_USERNAME'],
                              password=config['INFLUXDB_PASSWORD'],
                              db=config['INFLUXDB_DB']) as client:
        icmp_redis_pool = await create_redis_pool(
            (config['REDIS_HOST'], int(config['REDIS_PORT'])),
            db=int(config['REDIS_DB']))

        snmp_redis_pool = await create_redis_pool(
            (config['REDIS_HOST'], int(config['REDIS_PORT'])),
            db=int(config['REDIS_DB']))

        icmp_queue = AioRedisQueue(redis=icmp_redis_pool,
                                   queue_name=config['ICMP_QUEUE_NAME'])

        snmp_queue = AioRedisQueue(redis=snmp_redis_pool,
                                   queue_name=config['SNMP_QUEUE_NAME'])

        tasks = []
        for _ in range(int(config['TASKS'])):
            icmp_task = asyncio.create_task(
                influx_callback(icmp_worker(icmp_queue), client))
            tasks.append(icmp_task)

        for _ in range(int(config['TASKS'])):
            snmp_task = asyncio.create_task(
                influx_callback(snmp_worker(snmp_queue), client))
            tasks.append(snmp_task)

        await asyncio.gather(*tasks)


if __name__ == '__main__':
    args = docopt(__doc__, version='1.0')

    config_data = {**DEFAULT_CONFIG, **dotenv_values('.env'), **os.environ}

    config_data['TASKS'] = args['--tasks']

    logging.basicConfig(filename=args['--log-file'],
                        level=logging.WARNING,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        format='%(asctime)s - %(levelname)s - %(message)s')

    try:
        asyncio.run(main(config_data))
    except KeyboardInterrupt:
        sys.exit('Interrupted by user')
    except SystemExit:
        sys.exit('Interrupted by system')
