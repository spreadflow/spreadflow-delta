# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy
import errno
import os

try:
    import builtins
except ImportError:
    import __builtin__ as builtins

from mock import Mock, patch
from testtools import ExpectedException, TestCase

from spreadflow_core.scheduler import Scheduler
from spreadflow_delta.test.matchers import MatchesSendDeltaItemInvocation

from spreadflow_delta.proc import LockingProcessor, LockError, _Lockfile

class LockTestCase(TestCase):

    def test_lock_success(self):
        """
        Test the lockfile processor (happy path).
        """

        sut = LockingProcessor(key='test_lockpath')
        insert_proto = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'test_lockpath': '/path/to/some/file.txt.lck'
                }
            }
        }
        insert = copy.deepcopy(insert_proto)

        lock_mock = Mock(spec=_Lockfile)

        # Acquire lock
        expected = copy.deepcopy(insert_proto)
        matches = MatchesSendDeltaItemInvocation(expected, sut.out_locked)
        send = Mock(spec=Scheduler.send)

        with patch('spreadflow_delta.proc._Lockfile.open', return_value=lock_mock) as open_mock:
            sut(insert, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        open_mock.assert_called_once_with('/path/to/some/file.txt.lck')
        self.assertEquals(lock_mock.close.call_count, 0)

        # Release lock
        expected = copy.deepcopy(insert_proto)
        matches = MatchesSendDeltaItemInvocation(expected, sut.out)
        send = Mock(spec=Scheduler.send)

        sut.release(insert, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        lock_mock.close.assert_called_once_with()

    def test_lock_fail(self):
        """
        Test the lockfile processor (fail).
        """

        sut = LockingProcessor(key='test_lockpath')
        insert_proto = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'test_lockpath': '/path/to/some/file.txt.lck'
                }
            }
        }

        lock_mock = Mock(spec=_Lockfile)

        # Try-fail lock
        expected = {
            'inserts': [],
            'deletes': [],
            'data': {},
            'date': 'DATESTAMP'
        }
        insert = copy.deepcopy(insert_proto)
        update = copy.deepcopy(expected)
        matches = MatchesSendDeltaItemInvocation(expected, sut.out_retry)
        send = Mock(spec=Scheduler.send)

        sut._now = Mock(return_value='DATESTAMP')
        with patch('spreadflow_delta.proc._Lockfile.open', side_effect=LockError()) as open_mock:
            sut(insert, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        open_mock.assert_called_once_with('/path/to/some/file.txt.lck')
        self.assertEquals(lock_mock.close.call_count, 0)

        # Retry-success lock
        lock_mock = Mock(spec=_Lockfile)
        expected = copy.deepcopy(insert_proto)
        expected['date'] = 'DATESTAMP'
        matches = MatchesSendDeltaItemInvocation(expected, sut.out_locked)
        send = Mock(spec=Scheduler.send)

        with patch('spreadflow_delta.proc._Lockfile.open', return_value=lock_mock) as open_mock:
            sut(update, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        open_mock.assert_called_once_with('/path/to/some/file.txt.lck')
        self.assertEquals(lock_mock.close.call_count, 0)

        # Release lock
        expected = copy.deepcopy(insert_proto)
        expected['date'] = 'DATESTAMP'
        matches = MatchesSendDeltaItemInvocation(expected, sut.out)
        send = Mock(spec=Scheduler.send)

        sut.release(update, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        lock_mock.close.assert_called_once_with()

    def test_lock_detach(self):
        """
        Test that detach closes all acquired locks.
        """

        sut = LockingProcessor(key='test_lockpath')
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'test_lockpath': '/path/to/some/file.txt.lck'
                }
            }
        }

        lock_mock = Mock(spec=_Lockfile)

        # Acquire lock
        expected = copy.deepcopy(insert)
        matches = MatchesSendDeltaItemInvocation(expected, sut.out_locked)
        send = Mock(spec=Scheduler.send)

        with patch('spreadflow_delta.proc._Lockfile.open', return_value=lock_mock) as open_mock:
            sut(insert, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        open_mock.assert_called_once_with('/path/to/some/file.txt.lck')
        self.assertEquals(lock_mock.close.call_count, 0)

        # Call detach
        sut.detach()

        lock_mock.close.assert_called_once_with()

    @patch('os.open', return_value=99)
    def test_lock_helper(self, open_mock):
        expect_path = '/path/to/some/file.txt.lck'
        expect_flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        expect_mode = 0o600

        sut = _Lockfile.open('/path/to/some/file.txt.lck')

        open_mock.assert_called_once_with(expect_path, expect_flags, expect_mode)

        unlink_mock = Mock()
        close_mock = Mock()
        sut.close(os_unlink=unlink_mock, os_close=close_mock)

        unlink_mock.assert_called_once_with(expect_path)
        close_mock.assert_called_once_with(99)

    @patch('os.open', side_effect=OSError(errno.EAGAIN, os.strerror(errno.EAGAIN)))
    def test_lock_helper_eagain(self, open_mock):
        with ExpectedException(LockError):
            sut = _Lockfile.open('/path/to/some/file.txt.lck')

    @patch('os.open', side_effect=OSError(errno.EEXIST, os.strerror(errno.EEXIST)))
    def test_lock_helper_eexist(self, open_mock):
        with ExpectedException(LockError):
            sut = _Lockfile.open('/path/to/some/file.txt.lck')

    @patch('os.open', side_effect=OSError(errno.EDEADLK, os.strerror(errno.EDEADLK)))
    def test_lock_helper_edeadlk(self, open_mock):
        with ExpectedException(LockError):
            sut = _Lockfile.open('/path/to/some/file.txt.lck')

    @patch('os.open', side_effect=OSError(errno.ENOENT, os.strerror(errno.ENOENT)))
    def test_lock_helper_enoent(self, open_mock):
        with ExpectedException(LockError):
            sut = _Lockfile.open('/path/to/some/file.txt.lck')
