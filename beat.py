""" Task scheduler.

Usage:
  beat.py [options] [--community=TEXT] <NETWORK>
  beat.py -h | --help
  beat.py --version

Options:
  -h --help         Show this screen.
  --version         Show version.
  --community=TEXT  Set SNMP community [default: public].
  --log-file=FILE   Set name of logging file."""
import os
import logging
import asyncio
from typing import Any, Dict
from docopt import docopt
from dotenv import dotenv_values
import aioschedule as schedule
from aioredis import create_redis_pool
from redisqueue import AioRedisQueue
from schedules import find_hosts, check_icmp, check_system, check_interfaces

DEFAULT_CONFIG = {
    'REDIS_HOST': 'localhost',
    'REDIS_PORT': 6379,
    'REDIS_DB': 0,
    'ICMP_QUEUE_NAME': 'queue:icmp',
    'SNMP_QUEUE_NAME': 'queue:snmp',
    'COMMUNITY': 'public',
    'NETWORK': 'localhost'
}


async def main(config: Dict[str, Any]) -> None:
    """ Main schedule
    """
    redis_pool = await create_redis_pool(
        (config['REDIS_HOST'], int(config['REDIS_PORT'])),
        db=int(config['REDIS_DB']))

    icmp_queue = AioRedisQueue(redis=redis_pool,
                               queue_name=config['ICMP_QUEUE_NAME'])

    snmp_queue = AioRedisQueue(redis=redis_pool,
                               queue_name=config['SNMP_QUEUE_NAME'])

    schedule.every(30).seconds.do(check_icmp, queue=icmp_queue)

    schedule.every(1).minutes.do(check_system,
                                 queue=snmp_queue,
                                 community=config['COMMUNITY'])

    schedule.every(1).minutes.do(check_interfaces,
                                 queue=snmp_queue,
                                 community=config['COMMUNITY'])

    schedule.every(5).minutes.do(find_hosts,
                                 redis=redis_pool,
                                 network=config['NETWORK'])

    await schedule.run_all()

    while True:
        await schedule.run_pending()
        await asyncio.sleep(1)


if __name__ == '__main__':
    args = docopt(__doc__, version='1.0')

    config_data = {**DEFAULT_CONFIG, **dotenv_values('.env'), **os.environ}

    config_data['NETWORK'] = args['<NETWORK>']
    config_data['COMMUNITY'] = args['--community']

    logging.basicConfig(filename=args['--log-file'],
                        level=logging.WARNING,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        format='%(asctime)s - %(levelname)s - %(message)s')

    try:
        asyncio.run(main(config_data))
    except KeyboardInterrupt:
        logging.info('Interrupted by user')
    except SystemExit:
        logging.info('Interrupted by system')
