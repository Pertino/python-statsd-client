# -*- coding: utf-8 -*-
#
# This file is part of python-statsd-client released under the Apache
# License, Version 2.0. See the NOTICE for more information.

from __future__ import absolute_import
from functools import wraps
import random
from socket import socket, AF_INET, SOCK_DGRAM
import time
import logging

__version__ = '1.0.4'

STATSD_HOST = 'localhost'
STATSD_PORT = 8125
STATSD_SAMPLE_RATE = None
STATSD_BUCKET_PREFIX = None


def decrement(bucket, delta=1, sample_rate=None):
    _statsd.decr(bucket, delta, sample_rate)


def increment(bucket, delta=1, sample_rate=None):
    _statsd.incr(bucket, delta, sample_rate)


def gauge(bucket, value, sample_rate=None):
    _statsd.gauge(bucket, value, sample_rate)


def timing(bucket, ms, sample_rate=None):
    _statsd.timing(bucket, ms, sample_rate)


class StatsdClient(object):

    def __init__(self, host=None, port=None, prefix=None, sample_rate=None, emitter=None):
        self._host = host or STATSD_HOST
        self._port = port or STATSD_PORT
        self._sample_rate = sample_rate or STATSD_SAMPLE_RATE
        self._emitter = emitter
        self._socket = socket(AF_INET, SOCK_DGRAM)
        self._prefix = prefix or STATSD_BUCKET_PREFIX
        if self._prefix and not isinstance(self._prefix, bytes):
            self._prefix = self._prefix.encode('utf8')

    def timer(self, bucket):
        return StatsdTimer(bucket, statsd_client=self)

    def counter(self, bucket):
        return StatsdCounter(bucket, statsd_client=self)

    def decr(self, bucket, delta=1, sample_rate=None):
        """Decrements a counter by delta.
        """
        value = -1 * delta
        postfix = b'|c'
        self._send(bucket, value, postfix, sample_rate)

    def incr(self, bucket, delta=1, sample_rate=None):
        """Increment a counter by delta.
        """
        value = delta
        postfix = b'|c'
        self._send(bucket, value, postfix, sample_rate)

    def gauge(self, bucket, value, sample_rate=None):
        """Send a gauge value.
        """
        postfix = b'|g'
        self._send(bucket, value, postfix, sample_rate)

    def timing(self, bucket, ms, sample_rate=None):
        """Creates a timing sample.
        """
        value = ms
        postfix = b'|ms'
        self._send(bucket, value, postfix, sample_rate)

    def _socket_send(self, stat):
        self._socket.sendto(stat, (self._host, self._port))

    def _send(self, bucket, value, postfix, sample_rate=None):
        """Format and send data to statsd.
        """
        try:
            bucket = bucket if isinstance(bucket, bytes) else bucket.encode('utf8')

            sample_rate = sample_rate or self._sample_rate
            if sample_rate and (0 < sample_rate < 1.0):
                if random.random() <= sample_rate:
                    value = value + b'|@' + str(sample_rate).encode('utf8')
                else:
                    return

            if self._prefix:
                bucket = self._prefix + b'.' + bucket

            if self._emitter:
                self._emitter.put_stat(bucket, value, postfix)
            else:
                stat = bucket + b':' + str(value).encode('utf8') + postfix
                self._socket_send(stat)

        except Exception, e:
            _logger.error("Failed to send statsd packet.", exc_info=True)




class StatsdCounter(object):
    """Counter for StatsD.
    """
    def __init__(self, bucket, statsd_client=None):
        self._client = statsd_client or _statsd
        self._bucket = bucket if isinstance(bucket, bytes) else bucket.encode('utf8')

    def __add__(self, num):
        self._client.incr(self._bucket, delta=num)
        return self

    def __sub__(self, num):
        self._client.decr(self._bucket, delta=num)
        return self


class StatsdTimer(object):
    """Timer for StatsD.
    """
    def __init__(self, bucket, statsd_client=None):
        self._client = statsd_client or _statsd
        self._bucket = bucket if isinstance(bucket, bytes) else bucket.encode('utf8')

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        if type is not None:
            self.stop(b'total-except')
        else:
            self.stop()

    def start(self, bucket_key=b'start'):
        """Start the timer.
        """
        bucket_key = bucket_key if isinstance(bucket_key, bytes) else bucket_key.encode('utf8')
        self._start = time.time() * 1000
        self._splits = [(bucket_key, self._start), ]

    def split(self, bucket_key):
        """Records time since start() or last call to split() and sends
        result to statsd.
        """
        bucket_key = bucket_key if isinstance(bucket_key, bytes) else bucket_key.encode('utf8')
        self._splits.append((bucket_key, time.time() * 1000))
        self._client.timing(self._bucket + b'.' + bucket_key,
                            self._splits[-1][1] - self._splits[-2][1])

    def stop(self, bucket_key=b'total'):
        """Stops the timer and sends total time to statsd.
        """
        bucket_key = bucket_key if isinstance(bucket_key, bytes) else bucket_key.encode('utf8')
        self._stop = time.time() * 1000
        self._client.timing(self._bucket + b'.' + bucket_key,
                            self._stop - self._start)

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kw):
            with self:
                return func(*args, **kw)
        return wrapper


def init_statsd(settings=None):
    """Initialize global statsd client.
    """
    global _statsd
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


    _statsd = StatsdClient(host=STATSD_HOST, port=STATSD_PORT,
                           sample_rate=STATSD_SAMPLE_RATE, prefix=STATSD_BUCKET_PREFIX)
    return _statsd

_logger = logging.getLogger('statsd')
_statsd = init_statsd()
