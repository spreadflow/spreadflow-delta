# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy

try:
    import builtins
except ImportError:
    import __builtin__ as builtins

from mock import Mock, mock_open, patch
from testtools import TestCase

from spreadflow_core.scheduler import Scheduler
from spreadflow_delta.test.matchers import MatchesSendDeltaItemInvocation

from spreadflow_delta.proc import Loadfile

class LoadfileTestCase(TestCase):

    def test_loadfile(self):
        """
        Test the loadfile processor.
        """

        def _lower(key, doc):
            doc['lower'] = doc['orig'].lower()

        sut = Loadfile(key='test_path', destkey='test_content', encoding='utf-8')
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'test_path': '/path/to/some/file.txt'
                }
            }
        }
        expected = copy.deepcopy(insert)
        expected['data']['a']['test_content'] = 'rändöm'
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)

        open_mock = mock_open()
        open_mock.return_value.read.side_effect = ('rändöm'.encode('utf-8'), b'')

        with patch.object(builtins, 'open', open_mock):
            sut(insert, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        open_mock.assert_called_once_with('/path/to/some/file.txt', 'rb', 1)
