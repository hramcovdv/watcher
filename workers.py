#!/usr/bin/env python3
""" Workers module
"""
from typing import Any, Text, List, Dict, Union, Optional, AsyncGenerator
import logging
from aioping import ping
from aiosnmp import Snmp
from aiosnmp.exceptions import SnmpTimeoutError, SnmpException
from aioinflux import InfluxDBClient, InfluxDBError
from redisqueue import AioRedisQueue
from serieshelper import SeriesHelper


async def do_ping(host: Text,
                  count: Optional[int] = 5,
                  timeout: Optional[int] = 5) -> AsyncGenerator:
    """ Do ICMP ping
    """
    while count:
        try:
            delay = await ping(host, timeout) * 1000
        except TimeoutError:
            yield 0.0
        else:
            yield delay

        count = count - 1


async def icmp_worker(queue: AioRedisQueue) -> AsyncGenerator:
    """ ICMP worker
    """
    while True:
        job: Any = await queue.get()

        if job is None:
            break

        results: List[Union[float, int]] = []
        async for delay in do_ping(host=job['address'],
                                   count=job.get('count', 5),
                                   timeout=job.get('timeout', 5)):
            results.append(delay)

        min_rtt, max_rtt = min(results), max(results)
        packets_sent = len(results)
        packets_received = len([i for i in results if i != 0.0])

        result: Dict[str, Union[float, int]] = {
            'min_rtt': min_rtt,
            'max_rtt': max_rtt,
            'avg_rtt': sum(results) / packets_sent,
            'packets_sent': packets_sent,
            'packets_received': packets_received
        }

        yield job, result


async def snmp_worker(queue: AioRedisQueue) -> AsyncGenerator:
    """ SNMP worker
    """
    while True:
        job: Any = await queue.get()

        if job is None:
            break

        results: Dict[str, Dict[str, Union[None, str, int, bytes]]] = {}

        try:
            async with Snmp(host=job['address'],
                            community=job.get('community', 'public'),
                            timeout=job.get('timeout', 1),
                            retries=job.get('retries', 3)) as snmp:
                for object_name, object_id in job['oids'].items():
                    for res in await snmp.bulk_walk(object_id):
                        object_index: Text = res.oid[len(object_id.lstrip('.')) + 1:].lstrip('.')
                        results.setdefault(object_index, {})
                        results[object_index][object_name] = res.value
        except SnmpTimeoutError:
            logging.info('%s - %s', job['address'], 'Timeout connection to host')
        except SnmpException as err:
            logging.warning('%s - %s', job['address'], err)
        else:
            yield job, list(results.values())


async def influx_callback(worker: AsyncGenerator, client: InfluxDBClient) -> None:
    """ InfluxDB callback
    """
    async for job, result in worker:
        points = SeriesHelper(series=job['series'], tags=job.get('tags'))

        if isinstance(result, dict):
            points.add_point(result)

        if isinstance(result, list):
            points.add_points(result)

        try:
            await client.write(points, host=job['address'])
        except InfluxDBError as error:
            logging.warning('%s - %s', job['address'], error)
        else:
            logging.info('%s - %s', job['address'], 'Job is done.')
