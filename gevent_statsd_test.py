# -*- coding: utf-8 -*-
#
# This file is part of python-statsd-client released under the Apache
# License, Version 2.0. See the NOTICE for more information.

import unittest
import time
from gevent.pool import Pool as gevent_pool
import gevent_statsd
import statsd
from mockito import *


class TestStatsd(unittest.TestCase):

    def setUp(self):
        settings = {'STATSD_HOST': '127.0.0.1',
                    'STATSD_PORT': 9999,
                    'STATSD_SAMPLE_RATE': 0.99,
                    'STATSD_BUCKET_PREFIX': 'testing',
                    'STATSD_GREEN_POOL_SIZE': 50}
        gevent_statsd.init_statsd(settings)

    def tearDown(self):
        gevent_statsd.STATSD_HOST = 'localhost'
        gevent_statsd.STATSD_PORT = 8125
        gevent_statsd.STATSD_SAMPLE_RATE = None
        gevent_statsd.STATSD_BUCKET_PREFIX = None
        gevent_statsd.STATSD_GREEN_POOL_SIZE = 0

    def test_init_statsd(self):
        self.assertEqual(gevent_statsd.STATSD_HOST, '127.0.0.1')
        self.assertEqual(gevent_statsd.STATSD_PORT, 9999)
        self.assertEqual(gevent_statsd.STATSD_SAMPLE_RATE, 0.99)
        self.assertEqual(gevent_statsd.STATSD_BUCKET_PREFIX, 'testing')
        self.assertEqual(gevent_statsd.STATSD_GREEN_POOL_SIZE, 50)

    def test_send_pool_is_full(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)

    def test_decrement(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        verify(mock_gevent_pool, never).spawn()

    def test_increment(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)
        gevent_statsd._statsd._send_pool = mock_gevent_pool

        gevent_statsd.increment('counted')
        verify(mock_gevent_pool).spawn(any(), b'testing.counted:1|c|@0.99', ('127.0.0.1', 9999))

        gevent_statsd.increment('counted', 5)
        verify(mock_gevent_pool).spawn(any(), b'testing.counted:5|c|@0.99', ('127.0.0.1', 9999))

        gevent_statsd.increment('counted', 5, 0.98)
        verify(mock_gevent_pool).spawn(any(), b'testing.counted:5|c|@0.98', ('127.0.0.1', 9999))


    def test_gauge(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)
        gevent_statsd._statsd._send_pool = mock_gevent_pool

        gevent_statsd.gauge('gauged', 1)
        verify(mock_gevent_pool).spawn(any(), b'testing.gauged:1|g|@0.99', ('127.0.0.1', 9999))

        gevent_statsd.gauge('gauged', 5)
        verify(mock_gevent_pool).spawn(any(), b'testing.gauged:5|g|@0.99', ('127.0.0.1', 9999))

        gevent_statsd.gauge('gauged', 5, 0.98)
        verify(mock_gevent_pool).spawn(any(), b'testing.gauged:5|g|@0.98', ('127.0.0.1', 9999))

    def test_timing(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)
        gevent_statsd._statsd._send_pool = mock_gevent_pool

        gevent_statsd.timing('timed', 250)
        verify(mock_gevent_pool).spawn(any(), b'testing.timed:250|ms|@0.99', ('127.0.0.1', 9999))

        gevent_statsd.timing('timed', 250, 0.98)
        verify(mock_gevent_pool).spawn(any(), b'testing.timed:250|ms|@0.98', ('127.0.0.1', 9999))


class TestStatsdClient(unittest.TestCase):

    def test_prefix(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)

        client = gevent_statsd.GEventStatsdClient('localhost', 8125, prefix='main.bucket', sample_rate=None)
        client._send_pool = mock_gevent_pool
        client._send(b'subname', b'100|c')
        verify(mock_gevent_pool).spawn(any(), b'main.bucket.subname:100|c', any())

        client = gevent_statsd.GEventStatsdClient('localhost', 8125, prefix='main', sample_rate=None)
        client._send_pool = mock_gevent_pool
        client._send(b'subname', b'100|c')
        verify(mock_gevent_pool).spawn(any(), b'main.subname:100|c', any())
        client._send(b'subname.subsubname', b'100|c')
        verify(mock_gevent_pool).spawn(any(), b'main.subname.subsubname:100|c', any())

    def test_decr(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)

        client = gevent_statsd.GEventStatsdClient('localhost', 8125, prefix='', sample_rate=None)
        client._send_pool = mock_gevent_pool
        client.decr('buck.counter', 5)
        verify(mock_gevent_pool).spawn(any(), b'buck.counter:-5|c', any())

    def test_decr_sample_rate(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)

        client = gevent_statsd.GEventStatsdClient('localhost', 8125, prefix='', sample_rate=0.999)
        client._send_pool = mock_gevent_pool
        client.decr('buck.counter', 5)
        verify(mock_gevent_pool).spawn(any(), b'buck.counter:-5|c|@0.999', any())

    def test_incr(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)

        client = gevent_statsd.GEventStatsdClient('localhost', 8125, prefix='', sample_rate=None)
        client._send_pool = mock_gevent_pool
        client.incr('buck.counter', 5)
        verify(mock_gevent_pool).spawn(any(), b'buck.counter:5|c', any())

    def test_incr_sample_rate(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)

        client = gevent_statsd.GEventStatsdClient('localhost', 8125, prefix='', sample_rate=0.999)
        client._send_pool = mock_gevent_pool
        client.incr('buck.counter', 5)
        verify(mock_gevent_pool).spawn(any(), b'buck.counter:5|c|@0.999', any())

    def test_send(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)

        client = gevent_statsd.GEventStatsdClient('localhost', 8125, prefix='', sample_rate=None)
        client._send_pool = mock_gevent_pool
        client._send(b'buck', b'50|c')
        verify(mock_gevent_pool).spawn(any(), b'buck:50|c', any())

    def test_send_sample_rate(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)

        client = gevent_statsd.GEventStatsdClient('localhost', 8125, prefix='', sample_rate=0.999)
        client._send_pool = mock_gevent_pool
        client._send(b'buck', b'50|c')
        verify(mock_gevent_pool).spawn(any(), b'buck:50|c|@0.999', any())

    def test_timing(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)

        client = gevent_statsd.GEventStatsdClient('localhost', 8125, prefix='', sample_rate=None)
        client._send_pool = mock_gevent_pool
        client.timing('buck.timing', 100)
        verify(mock_gevent_pool).spawn(any(), b'buck.timing:100|ms', any())

    def test_timing_sample_rate(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)

        client = gevent_statsd.GEventStatsdClient('localhost', 8125, prefix='', sample_rate=0.999)
        client._send_pool = mock_gevent_pool
        client.timing('buck.timing', 100)
        verify(mock_gevent_pool).spawn(any(), b'buck.timing:100|ms|@0.999', any())


class TestStatsdCounter(unittest.TestCase):

    def test_add(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)
        client = gevent_statsd.GEventStatsdClient('localhost', 8125, prefix='', sample_rate=None)
        client._send_pool = mock_gevent_pool
        counter = statsd.StatsdCounter('counted', client)
        counter += 1
        verify(mock_gevent_pool).spawn(any(), b'counted:1|c', any())
        counter += 5
        verify(mock_gevent_pool).spawn(any(), b'counted:5|c', any())

    def test_sub(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)
        client = gevent_statsd.GEventStatsdClient('localhost', 8125, prefix='', sample_rate=None)
        client._send_pool = mock_gevent_pool
        counter = statsd.StatsdCounter('counted', client)
        counter -= 1
        verify(mock_gevent_pool).spawn(any(), b'counted:-1|c', any())
        counter -= 5
        verify(mock_gevent_pool).spawn(any(), b'counted:-5|c', any())


class TestStatsdTimer(unittest.TestCase):


    def test_startstop(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)

        client = gevent_statsd.GEventStatsdClient('localhost', 8125, prefix='', sample_rate=None)
        client._send_pool = mock_gevent_pool
        timer = statsd.StatsdTimer('timeit', client)
        timer.start()
        time.sleep(0.25)
        timer.stop()
        verify(mock_gevent_pool).spawn(any(), contains(b'timeit.total:'), any())

    def test_split(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)

        client = gevent_statsd.GEventStatsdClient('localhost', 8125, prefix='', sample_rate=None)
        client._send_pool = mock_gevent_pool
        timer = statsd.StatsdTimer('timeit', client)
        timer.start()
        time.sleep(0.25)
        timer.split('lap')
        verify(mock_gevent_pool).spawn(any(), contains(b'timeit.lap:'), any())

        time.sleep(0.26)
        timer.stop()
        verify(mock_gevent_pool).spawn(any(), contains(b'timeit.total:'), any())

    def test_wrap(self):
        class TC(object):
            @gevent_statsd.StatsdTimer('timeit')
            def do(self):
                time.sleep(0.25)
                return 1
        tc = TC()
        result = tc.do()
        self.assertEqual(result, 1)

    def test_with(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)
        client = gevent_statsd.GEventStatsdClient('localhost', 8125, prefix='', sample_rate=None)
        client._send_pool = mock_gevent_pool

        timer = statsd.StatsdTimer('timeit', client)
        with timer:
            time.sleep(0.25)
        verify(mock_gevent_pool).spawn(any(), contains(b'timeit.total:'), any())

if __name__ == '__main__':
    unittest.main()
