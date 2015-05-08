# -*- coding: utf-8 -*-
#
# This file is part of python-statsd-client released under the Apache
# License, Version 2.0. See the NOTICE for more information.

from gevent.pool import Pool
from gevent.socket import socket
from gevent import Greenlet
from gevent import sleep as gSleep
from gevent.queue import Queue as gQueue
from gevent.event import Event as gEvent

from socket import AF_INET, SOCK_DGRAM
from statsd import _statsd, StatsdClient
from statsd import StatsdCounter as StatsdCounterBase
from statsd import StatsdTimer as StatsdTimerBase

import time
import logging

_logger = logging.Logger(__name__)

STATSD_HOST = 'localhost'
STATSD_PORT = 8125
STATSD_SAMPLE_RATE = None
STATSD_BUCKET_PREFIX = None
STATSD_GREEN_POOL_SIZE = 50
STATSD_USE_EMITTER = False
STATSD_EMIT_INTERVAL = 5
STATSD_MAX_STAT_EMIT_COUNT = 50

def decrement(bucket, delta=1, sample_rate=None):
    _statsd.decr(bucket, delta, sample_rate)

def increment(bucket, delta=1, sample_rate=None):
    _statsd.incr(bucket, delta, sample_rate)

def gauge(bucket, value, sample_rate=None):
    _statsd.gauge(bucket, value, sample_rate)

def timing(bucket, ms, sample_rate=None):
    _statsd.timing(bucket, ms, sample_rate)


class GEventStatsdUDPEmitter(Greenlet):

    stats_ready = gEvent()
    stats_queue = gQueue()

    @staticmethod
    def put_stat(bucket, value, postfix):
        GEventStatsdUDPEmitter.stats_queue.put((bucket, value, postfix))
        GEventStatsdUDPEmitter.stats_ready.set()

    def __init__(self, host, port, max_stat_count_payload=None, emit_interval_seconds=None):
        self._host, self.port = host, port
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._max_payload = max_stat_count_payload or 50
        self._emit_interval = emit_interval_seconds or 5

    def _run(self):
        self.running = True
        last_emit = time.time()
        while self.running:
            GEventStatsdUDPEmitter.stats_ready.wait(self._emit_interval - (time.time() - last_emit))
            # temporarily store here for aggregation and averaging
            stats = {b'|c': dict(), b'|g': dict(), b'|ms': dict()}
            # need to know the occurrence of each timer for average's sake
            timers_occ = dict()
            # count of stats
            count = 0

            # while stats on queue, max payload not exceed, and emit interval not reached
            while not GEventStatsdUDPEmitter.stats_queue.empty() \
                    and count < self.max_payload \
                    and time.time() - self._last_emit < self.emit_interval:
                bucket, value, postfix = GEventStatsdUDPEmitter.stats_queue.get()
                if postfix == b'|ms':
                    # we need to average the timers, so what was it
                    prev_time = stats[postfix].get(bucket, 0)
                    # total time of all the times this thing has reported
                    sum_time = prev_time * timers_occ.get(bucket, 0) + value
                    # number of times this thing has reported
                    timers_occ[bucket] = timers_occ.get(bucket, 0) + 1
                    # average time
                    avg_time = sum_time / timers_occ[bucket]
                    # stash it
                    stats[postfix][bucket] = avg_time
                else:
                    # incr or decr
                    stats[postfix][bucket] = value + stats[postfix].get(bucket, 0)
                gSleep(0)

            if count != 0:
                payload = '\n'.join('\n'.join('{bucket}:{value}{postfix}'.format(bucket=bucket, value=value, postfix=stat_type)
                                              for bucket, value in stat_map.iteritems()) for stat_type, stat_map in stats.iteritems())

                self._socket.sendto(payload.strip(), (self._host, self._port))
                last_emit = time.time()

            if GEventStatsdUDPEmitter.stats_queue.empty():
                GEventStatsdUDPEmitter.stats_ready.clear()


class GEventStatsdClient(StatsdClient):
    """ GEvent Enabled statsd client
    """
    def __init__(self, pool_size=None,
                 host=None, port=None, prefix=None, sample_rate=None, emitter=None):
        """
        Create GEvent enabled statsd client
        :param pool_size: Option size of the greenlet pool
        :param host: hostname for the statsd server
        :param port: port for the statsd server
        :param prefix: user defined prefix
        :param sample_rate: rate to which stats are dropped
        """
        super(GEventStatsdClient, self).__init__(host, port, prefix, sample_rate, emitter)
        self._send_pool = Pool(pool_size or STATSD_GREEN_POOL_SIZE)
        self._socket = socket(AF_INET, SOCK_DGRAM)

    def _socket_send(self, stat):
        """
        Override the subclasses send method to schedule a udp write.
        :param stat: Stat string to write
        """
        # if we exceed the pool we drop the stat on the floor
        _logger.debug("This is the gevent socket send! Data: %s", stat)
        if not self._send_pool.full():
            # We can't monkey patch this as we don't want to ever block the calling greenlet
            self._send_pool.spawn(self._socket.sendto, stat, (self._host, self._port))


class StatsdCounter(StatsdCounterBase):
    """GEvent version of the Counter for StatsD.
    """
    def __init__(self, bucket, statsd_client=None):
        super(StatsdCounter, self).__init__(bucket)
        self._client = statsd_client or GEventStatsdClient()


class StatsdTimer(StatsdTimerBase):
    """GEvent version of the Timer for StatsD.
    """
    def __init__(self, bucket, statsd_client=None):
        super(StatsdTimer, self).__init__(bucket)
        self._client = statsd_client or GEventStatsdClient()


def monkey_patch_statsd():
    # assuming that you are initializing this module you want to overload the existing statsd.
    # Let me go ahead and monkey patch that for you.
    import statsd
    statsd._statsd = _statsd


def init_statsd(settings=None):
    """Initialize global gevent statsd client.
    """
    global _statsd
    global STATSD_GREEN_POOL_SIZE
    global STATSD_HOST
    global STATSD_PORT
    global STATSD_SAMPLE_RATE
    global STATSD_BUCKET_PREFIX
    global STATSD_USE_EMITTER
    global STATSD_EMIT_INTERVAL
    global STATSD_MAX_STAT_EMIT_COUNT

    if settings:
        STATSD_HOST = settings.get('STATSD_HOST', STATSD_HOST)
        STATSD_PORT = settings.get('STATSD_PORT', STATSD_PORT)
        STATSD_SAMPLE_RATE = settings.get('STATSD_SAMPLE_RATE',
                                          STATSD_SAMPLE_RATE)
        STATSD_BUCKET_PREFIX = settings.get('STATSD_BUCKET_PREFIX',
                                            STATSD_BUCKET_PREFIX)
        STATSD_GREEN_POOL_SIZE = settings.get('STATSD_GREEN_POOL_SIZE',
                                              STATSD_GREEN_POOL_SIZE)

        STATSD_USE_EMITTER = settings.get('STATSD_USE_EMITTER', STATSD_USE_EMITTER)
        STATSD_EMIT_INTERVAL = settings.get('STATSD_EMIT_INTERVAL', STATSD_EMIT_INTERVAL)
        STATSD_MAX_STAT_EMIT_COUNT = settings.get('STATSD_MAX_STAT_EMIT_COUNT', STATSD_MAX_STAT_EMIT_COUNT)

    _statsd = GEventStatsdClient(host=STATSD_HOST, port=STATSD_PORT,
                                 sample_rate=STATSD_SAMPLE_RATE, prefix=STATSD_BUCKET_PREFIX,
                                 emitter=[None, GEventStatsdUDPEmitter][STATSD_USE_EMITTER])
    monkey_patch_statsd()

    _statsd_emitter = None

    if STATSD_USE_EMITTER:
        _statsd_emitter = GEventStatsdUDPEmitter(STATSD_HOST, STATSD_PORT)
        _statsd_emitter.start()

    return _statsd, _statsd_emitter


def getStatsd(name):
    full_namespace = '{existing_prefix}.{caller_name}'.format(existing_prefix=_statsd._prefix, caller_name=name)
    _statsd_sub = GEventStatsdClient(prefix=full_namespace)

    # attrs from initial init_statsd() call
    # use the same pool
    for key in ['_host', '_port', '_sample_rate', '_send_pool']:
        _statsd_sub.__dict__[key] = _statsd.__dict__[key]



_logger = logging.getLogger('statsd')
_statsd, _statsd_emitter = init_statsd()
monkey_patch_statsd()
