# -*- coding: utf-8 -*-
#
# This file is part of python-statsd-client released under the Apache
# License, Version 2.0. See the NOTICE for more information.

from gevent.pool import Pool
from gevent.socket import socket
from socket import AF_INET, SOCK_DGRAM
from statsd import _statsd, StatsdClient
from statsd import StatsdCounter as StatsdCounterBase
from statsd import StatsdTimer as StatsdTimerBase

import logging

_logger = logging.Logger(__name__)

STATSD_HOST = 'localhost'
STATSD_PORT = 8125
STATSD_SAMPLE_RATE = None
STATSD_BUCKET_PREFIX = None
STATSD_GREEN_POOL_SIZE = 50


def decrement(bucket, delta=1, sample_rate=None):
    _statsd.decr(bucket, delta, sample_rate)

def increment(bucket, delta=1, sample_rate=None):
    _statsd.incr(bucket, delta, sample_rate)

def gauge(bucket, value, sample_rate=None):
    _statsd.gauge(bucket, value, sample_rate)

def timing(bucket, ms, sample_rate=None):
    _statsd.timing(bucket, ms, sample_rate)

class GEventStatsdClient(StatsdClient):
    """ GEvent Enabled statsd client
    """
    def __init__(self, pool_size=None,
                 host=None, port=None, prefix=None, sample_rate=None):
        """
        Create GEvent enabled statsd client
        :param pool_size: Option size of the greenlet pool
        :param host: hostname for the statsd server
        :param port: port for the statsd server
        :param prefix: user defined prefix
        :param sample_rate: rate to which stats are dropped
        """
        super(GEventStatsdClient, self).__init__(host, port, prefix, sample_rate)
        self._send_pool = Pool(pool_size or STATSD_GREEN_POOL_SIZE)
        self._socket = socket(AF_INET, SOCK_DGRAM)

    def _socket_send(self, stat):
        """
        Override the subclasses send method to schedule a udp write.
        :param stat: Stat string to write
        """
        # if we exceed the pool we drop the stat on the floor
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

    if settings:
        STATSD_HOST = settings.get('STATSD_HOST', STATSD_HOST)
        STATSD_PORT = settings.get('STATSD_PORT', STATSD_PORT)
        STATSD_SAMPLE_RATE = settings.get('STATSD_SAMPLE_RATE',
                                          STATSD_SAMPLE_RATE)
        STATSD_BUCKET_PREFIX = settings.get('STATSD_BUCKET_PREFIX',
                                            STATSD_BUCKET_PREFIX)
        STATSD_GREEN_POOL_SIZE = settings.get('STATSD_GREEN_POOL_SIZE',
                                              STATSD_GREEN_POOL_SIZE)
    _statsd = GEventStatsdClient(host=STATSD_HOST, port=STATSD_PORT,
                                 sample_rate=STATSD_SAMPLE_RATE, prefix=STATSD_BUCKET_PREFIX)
    monkey_patch_statsd()
    return _statsd




_logger = logging.getLogger('statsd')
_statsd = init_statsd()
monkey_patch_statsd()
