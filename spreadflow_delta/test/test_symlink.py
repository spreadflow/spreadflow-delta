# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy

from mock import Mock, patch
from testtools import TestCase

from spreadflow_core.scheduler import Scheduler
from spreadflow_delta.test.matchers import MatchesSendDeltaItemInvocation

from spreadflow_delta.proc import Symlink

class SymlinkTestCase(TestCase):

    def test_symlink(self):
        """
        Test the symlink processor.
        """

        sut = Symlink(key='test_path', destkey='test_linkpath')
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'test_path': '/path/to/some/file.txt',
                    'test_linkpath': '/path/to/symlink/test.txt'
                }
            }
        }
        expected = copy.deepcopy(insert)
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)

        with patch('os.symlink') as symlink_mock:
            sut(insert, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        symlink_mock.assert_called_once_with('/path/to/some/file.txt', '/path/to/symlink/test.txt')
