# -*- coding: utf-8 -*-
#
# This file is part of python-statsd-client released under the Apache
# License, Version 2.0. See the NOTICE for more information.

import unittest
import time

from gevent.pool import Pool as gevent_pool
from gevent.queue import Queue as gevent_queue
from gevent import sleep as gevent_sleep
from gevent.socket import socket as gevent_socket

import gevent_statsd
import statsd

from mockito import *


class TestStatsd(unittest.TestCase):

    def setUp(self):
        settings = {'STATSD_HOST': '127.0.0.1',
                    'STATSD_PORT': 9999,
                    'STATSD_SAMPLE_RATE': 0.99,
                    'STATSD_BUCKET_PREFIX': 'testing',
                    'STATSD_GREEN_POOL_SIZE': 50,
                    'STATSD_USE_EMITTER': False,
                    'STATSD_EMIT_INTERVAL': 1,
                    'STATSD_MAX_STAT_EMIT_COUNT': 5}

        gevent_statsd.init_statsd(settings)

    def tearDown(self):
        gevent_statsd.STATSD_HOST = 'localhost'
        gevent_statsd.STATSD_PORT = 8125
        gevent_statsd.STATSD_SAMPLE_RATE = None
        gevent_statsd.STATSD_BUCKET_PREFIX = None
        gevent_statsd.STATSD_GREEN_POOL_SIZE = 0
        gevent_statsd.STATSD_USE_EMITTER = False
        gevent_statsd.STATSD_EMIT_INTERVAL = 5
        gevent_statsd.STATSD_MAX_STAT_EMIT_COUNT = 50

    def test_init_statsd(self):
        self.assertEqual(gevent_statsd.STATSD_HOST, '127.0.0.1')
        self.assertEqual(gevent_statsd.STATSD_PORT, 9999)
        self.assertEqual(gevent_statsd.STATSD_SAMPLE_RATE, 0.99)
        self.assertEqual(gevent_statsd.STATSD_BUCKET_PREFIX, 'testing')
        self.assertEqual(gevent_statsd.STATSD_GREEN_POOL_SIZE, 50)
        self.assertEqual(gevent_statsd.STATSD_USE_EMITTER, False)
        self.assertEqual(gevent_statsd.STATSD_EMIT_INTERVAL, 1)
        self.assertEqual(gevent_statsd.STATSD_MAX_STAT_EMIT_COUNT, 5)

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
        client._send(b'subname', 100, b'|c')
        verify(mock_gevent_pool).spawn(any(), b'main.bucket.subname:100|c', any())

        client = gevent_statsd.GEventStatsdClient('localhost', 8125, prefix='main', sample_rate=None)
        client._send_pool = mock_gevent_pool
        client._send(b'subname', 100, b'|c')
        verify(mock_gevent_pool).spawn(any(), b'main.subname:100|c', any())
        client._send(b'subname.subsubname', 100, b'|c')
        verify(mock_gevent_pool).spawn(any(), b'main.subname.subsubname:100|c', any())

    def test_sub_clients_prefix_is_correct_and_attributes_reference_root_statsd_client_attributes(self):
        gevent_statsd.init_statsd(dict(STATSD_BUCKET_PREFIX='testing'))
        c = gevent_statsd.getStatsd('bender')
        self.assertEqual(c._prefix, 'testing.bender')

        for key in ['_host', '_port', '_sample_rate', '_send_pool']:
            self.assertTrue(c.__dict__[key] is gevent_statsd._statsd.__dict__[key])


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
        client._send(b'buck', 50, '|c')
        verify(mock_gevent_pool).spawn(any(), b'buck:50|c', any())

    def test_send_sample_rate(self):
        mock_gevent_pool = mock(gevent_pool)
        when(mock_gevent_pool).full().thenReturn(False)
        when(mock_gevent_pool).spawn(any(), any(), any()).thenReturn(None)

        client = gevent_statsd.GEventStatsdClient('localhost', 8125, prefix='', sample_rate=0.999)
        client._send_pool = mock_gevent_pool
        client._send(b'buck', 50, '|c')
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

    def test_put_stat_in_gevent_queue(self):
        gevent_statsd.STATSD_USE_EMITTER = True
        gevent_statsd.init_statsd()

        mock_gevent_queue = mock(gevent_queue)
        when(mock_gevent_queue).full().thenReturn(False)
        when(mock_gevent_queue).put(any()).thenReturn(None)

        client = gevent_statsd.GEventStatsdClient\
            ('localhost', 8125, prefix='', sample_rate=None, emitter=gevent_statsd._statsd_emitter)

        client._emitter._stats_queue = mock_gevent_queue

        client.timing('timing', 100)
        verify(mock_gevent_queue).put((b'timing', 100, b'|ms', None))

        client.timing('timing', 100, .99)
        verify(mock_gevent_queue).put((b'timing', 100, b'|ms', '|@0.99'))

        client.decr('decrement', 2)
        verify(mock_gevent_queue).put((b'decrement', -2, b'|c', None))

        client.incr('increment', 3)
        verify(mock_gevent_queue).put((b'increment', 3, b'|c', None))

        client.gauge('gauge', 55,)
        verify(mock_gevent_queue).put((b'gauge', 55, b'|g', None))



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


class TestGEventStatsUDPEmitter(unittest.TestCase):

    def setUp(self):
        self.settings = {'STATSD_HOST': '127.0.0.1',
                    'STATSD_PORT': 9999,
                    'STATSD_SAMPLE_RATE': 0.99,
                    'STATSD_BUCKET_PREFIX': 'testing',
                    'STATSD_GREEN_POOL_SIZE': 50,
                    'STATSD_USE_EMITTER': True,
                    'STATSD_EMIT_INTERVAL': 1,
                    'STATSD_MAX_STAT_EMIT_COUNT': 5}

        gevent_statsd.init_statsd(self.settings)

    def tearDown(self):
        gevent_statsd.STATSD_HOST = 'localhost'
        gevent_statsd.STATSD_PORT = 8125
        gevent_statsd.STATSD_SAMPLE_RATE = None
        gevent_statsd.STATSD_BUCKET_PREFIX = None
        gevent_statsd.STATSD_GREEN_POOL_SIZE = 0
        gevent_statsd.STATSD_USE_EMITTER = False
        gevent_statsd.STATSD_EMIT_INTERVAL = 5
        gevent_statsd.STATSD_MAX_STAT_EMIT_COUNT = 50

    def test_emitter_is_configured_properly(self):
        self.assertEqual(gevent_statsd._statsd_emitter._host, '127.0.0.1')
        self.assertEqual(gevent_statsd._statsd_emitter._port, 9999)
        self.assertIsInstance(gevent_statsd._statsd_emitter._socket, gevent_socket)
        self.assertEqual(gevent_statsd._statsd_emitter._max_payload, 5)
        self.assertEqual(gevent_statsd._statsd_emitter._emit_interval, 1)

    def test_emitter_can_start_and_stop(self):
        gevent_statsd.start_emitter()

        gevent_sleep(0)
        self.assertTrue(gevent_statsd._statsd_emitter.running)

        gevent_statsd.stop_emitter()
        gevent_sleep(gevent_statsd._statsd_emitter._emit_interval)
        self.assertFalse(gevent_statsd._statsd_emitter.running)

    def test_emitter_sends_stats_properly(self):
        mock_socket = mock(gevent_socket)
        when(gevent_socket).sendto(any(), any()).thenReturn(None)

        gevent_statsd._statsd_emitter._socket = mock_socket

        gevent_statsd.start_emitter()

        gevent_statsd._statsd_emitter.put_stat('statistic', 1, '|c', '|@0.99')
        gevent_sleep(gevent_statsd._statsd_emitter._emit_interval)
        verify(mock_socket).sendto('statistic:1|c|@0.99', ('127.0.0.1', 9999))

        gevent_statsd._statsd_emitter.put_stat('statistic', 1, '|c', None)
        gevent_sleep(gevent_statsd._statsd_emitter._emit_interval)
        verify(mock_socket).sendto('statistic:1|c', ('127.0.0.1', 9999))

        gevent_statsd._statsd_emitter.put_stat('statistic', 1, '|c', None)
        gevent_statsd._statsd_emitter.put_stat('statistic', 1, '|c', None)
        gevent_sleep(gevent_statsd._statsd_emitter._emit_interval)
        verify(mock_socket).sendto('statistic:2|c', ('127.0.0.1', 9999))

        gevent_statsd._statsd_emitter.put_stat('statistic', 1, '|c', None)
        gevent_statsd._statsd_emitter.put_stat('statistic', -3, '|c', None)
        gevent_statsd._statsd_emitter.put_stat('statistic', 1, '|c', '|@0.99')
        gevent_statsd._statsd_emitter.put_stat('statistic', -2, '|c', '|@0.99')
        gevent_statsd._statsd_emitter.put_stat('statistic', 4, '|c', None)
        gevent_sleep(gevent_statsd._statsd_emitter._emit_interval)
        verify(mock_socket, times(2)).sendto(contains('statistic:2|c'), ('127.0.0.1', 9999))

        gevent_statsd._statsd_emitter.put_stat('statistic', 1, '|c', None)
        gevent_statsd._statsd_emitter.put_stat('statistic', -3, '|c', None)
        gevent_statsd._statsd_emitter.put_stat('statistic', 1, '|c', '|@0.99')
        gevent_statsd._statsd_emitter.put_stat('statistic', -2, '|c', '|@0.99')
        gevent_statsd._statsd_emitter.put_stat('statistic', 4, '|c', None)
        gevent_sleep(gevent_statsd._statsd_emitter._emit_interval)
        verify(mock_socket, times(2)).sendto(contains('statistic:-1|c|@0.99'), ('127.0.0.1', 9999))

    def test_stats_propogate_through_emitter_socket(self):
        # turn off sample rate
        self.settings['STATSD_SAMPLE_RATE'] = None
        gevent_statsd.init_statsd(self.settings)

        mock_socket = mock(gevent_socket)
        when(gevent_socket).sendto(any(), any()).thenReturn(None)

        gevent_statsd._statsd_emitter._socket = mock_socket

        gevent_statsd.start_emitter()

        statsd.increment('some.bucket')
        statsd.increment('some.bucket', 2)
        statsd.increment('some.bucket', 5, .99)
        gevent_sleep(gevent_statsd._statsd_emitter._emit_interval)
        verify(mock_socket).sendto(contains('testing.some.bucket:3|c'), ('127.0.0.1', 9999))
        verify(mock_socket).sendto(contains('testing.some.bucket:5|c|@0.99'), ('127.0.0.1', 9999))

        statsd.decrement('some.bucket', 1)
        statsd.decrement('some.bucket', 5, .99)
        gevent_sleep(gevent_statsd._statsd_emitter._emit_interval)
        verify(mock_socket).sendto(contains('testing.some.bucket:-1|c'), ('127.0.0.1', 9999))
        verify(mock_socket).sendto(contains('testing.some.bucket:-5|c|@0.99'), ('127.0.0.1', 9999))

        statsd.gauge('some.gauge', 2)
        statsd.gauge('some.othergauge', 66, .99)
        gevent_sleep(gevent_statsd._statsd_emitter._emit_interval)
        verify(mock_socket).sendto(contains('testing.some.gauge:2|g'), ('127.0.0.1', 9999))
        verify(mock_socket).sendto(contains('testing.some.othergauge:66|g|@0.99'), ('127.0.0.1', 9999))

        statsd.timing('some.timer', 100)
        statsd.timing('some.timer', 200)
        statsd.timing('some.othertimer', 11, .99)
        statsd.timing('some.othertimer', 22, .99)
        gevent_sleep(gevent_statsd._statsd_emitter._emit_interval)
        verify(mock_socket).sendto(contains('testing.some.timer:150|ms'), ('127.0.0.1', 9999))
        verify(mock_socket).sendto(contains('testing.some.othertimer:16|ms|@0.99'), ('127.0.0.1', 9999))

    def test_emitter_obeys_max_stats_payload(self):

        # turn off sample rate
        self.settings['STATSD_SAMPLE_RATE'] = None
        self.settings['STATSD_MAX_STAT_EMIT_COUNT'] = 1
        gevent_statsd.init_statsd(self.settings)

        mock_socket = mock(gevent_socket)
        when(gevent_socket).sendto(any(), any()).thenReturn(None)

        gevent_statsd._statsd_emitter._socket = mock_socket

        gevent_statsd.start_emitter()

        for i in range(1, 11):
            statsd.increment('bucket', i)
            gevent_sleep(gevent_statsd._statsd_emitter._emit_interval)
            verify(mock_socket).sendto('testing.bucket:%d|c' % i, ('127.0.0.1', 9999))

    def test_emitter_does_math_precisely(self):
        # turn off sample rate
        self.settings['STATSD_SAMPLE_RATE'] = None
        self.settings['STATSD_MAX_STAT_EMIT_COUNT'] = 10
        gevent_statsd.init_statsd(self.settings)

        mock_socket = mock(gevent_socket)
        when(gevent_socket).sendto(any(), any()).thenReturn(None)

        gevent_statsd._statsd_emitter._socket = mock_socket

        gevent_statsd.start_emitter()

        # counters and gauges should be summed
        for i in range(1, 11):
            statsd.increment('bucket', i)

        gevent_sleep(gevent_statsd._statsd_emitter._emit_interval)
        verify(mock_socket).sendto('testing.bucket:55|c', ('127.0.0.1', 9999))

        for i in range(1, 11):
            statsd.gauge('bucket', i)

        gevent_sleep(gevent_statsd._statsd_emitter._emit_interval)
        verify(mock_socket).sendto('testing.bucket:55|g', ('127.0.0.1', 9999))

        # timers should be averaged
        for i in range(1, 11):
            statsd.timing('bucket', i)

        gevent_sleep(gevent_statsd._statsd_emitter._emit_interval)
        verify(mock_socket).sendto('testing.bucket:5|ms', ('127.0.0.1', 9999))




if __name__ == '__main__':
    unittest.main()
