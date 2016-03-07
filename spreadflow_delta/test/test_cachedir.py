# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy
import os
import shutil
import tempfile

from mock import Mock, patch
from testtools import TestCase

from spreadflow_core.scheduler import Scheduler
from spreadflow_delta.test.matchers import MatchesSendDeltaItemInvocation

from spreadflow_delta.proc import Cachedir

class CachedirTestCase(TestCase):

    def test_cachedir(self):
        """
        Test the cache directory processor.
        """

        sut = Cachedir(directory='/path/to/testdir', destkey='test_cachedir')

        # insert operation.
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {}
            }
        }
        expected = copy.deepcopy(insert)

        # >>> hashlib.sha1('a').hexdigest()
        # '86f7e437faa5a7fce15d1ddcb9eaeaea377667b8'
        expected_path = '/path/to/testdir/86/f7e437faa5a7fce15d1ddcb9eaeaea377667b8'
        expected['data']['a']['test_cachedir'] = expected_path

        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        with patch('tempfile.mkdtemp', spec=tempfile.mkdtemp) as mkdtemp_mock:
            with patch('os.makedirs', spec=os.makedirs) as makedirs_mock:
                with patch('shutil.rmtree', spec=shutil.rmtree) as rmtree_mock:
                    sut(insert, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        self.assertEquals(mkdtemp_mock.call_count, 0)
        rmtree_mock.assert_called_once_with(expected_path, ignore_errors=True)
        makedirs_mock.assert_called_once_with(expected_path)

        # delete operation.
        delete = {
            'inserts': [],
            'deletes': ['a'],
            'data': {
                'a': {}
            }
        }
        expected = copy.deepcopy(delete)

        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        with patch('tempfile.mkdtemp', spec=tempfile.mkdtemp) as mkdtemp_mock:
            with patch('os.makedirs', spec=os.makedirs) as makedirs_mock:
                with patch('shutil.rmtree', spec=shutil.rmtree) as rmtree_mock:
                    sut(delete, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        self.assertEquals(mkdtemp_mock.call_count, 0)
        rmtree_mock.assert_called_once_with(expected_path, ignore_errors=False)
        self.assertEquals(makedirs_mock.call_count, 0)

        # detach method
        with patch('shutil.rmtree', spec=shutil.rmtree) as rmtree_mock:
            sut.detach()

        self.assertEquals(rmtree_mock.call_count, 0)

    def test_tmp_cachedir(self):
        """
        Test the cache directory processor with temporary directory.
        """

        sut = Cachedir()
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {}
            }
        }
        expected = copy.deepcopy(insert)

        # >>> hashlib.sha1('a').hexdigest()
        # '86f7e437faa5a7fce15d1ddcb9eaeaea377667b8'
        expected_dir = '/path/to/tempdir'
        expected_path = expected_dir + '/86/f7e437faa5a7fce15d1ddcb9eaeaea377667b8'
        expected['data']['a']['cachedir'] = expected_path

        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        with patch('tempfile.mkdtemp', spec=tempfile.mkdtemp, return_value=expected_dir) as mkdtemp_mock:
            with patch('os.makedirs', spec=os.makedirs) as makedirs_mock:
                with patch('shutil.rmtree', spec=shutil.rmtree) as rmtree_mock:
                    sut(insert, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        mkdtemp_mock.assert_called_once_with()
        rmtree_mock.assert_called_once_with(expected_path, ignore_errors=True)
        makedirs_mock.assert_called_once_with(expected_path)

        with patch('shutil.rmtree', spec=shutil.rmtree) as rmtree_mock:
            sut.detach()

        rmtree_mock.assert_called_once_with(expected_dir, ignore_errors=True)
