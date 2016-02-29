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

from spreadflow_delta.proc import Savefile

class SavefileTestCase(TestCase):

    def test_savefile(self):
        """
        Test the savefile processor.
        """

        sut = Savefile(key='test_content', destkey='test_savepath', encoding='utf-8', clear=True)
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'test_savepath': '/path/to/some/file.txt',
                    'test_content': 'rändöm'
                }
            }
        }
        expected = copy.deepcopy(insert)
        del expected['data']['a']['test_content']
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)

        open_mock = mock_open()

        with patch.object(builtins, 'open', open_mock):
            sut(insert, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        open_mock.assert_called_once_with('/path/to/some/file.txt', 'wb', 1)
        open_mock.return_value.write.assert_called_once_with('rändöm'.encode('utf-8'))
