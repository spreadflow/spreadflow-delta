# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy

from mock import Mock
from testtools import TestCase

from spreadflow_core.scheduler import Scheduler
from spreadflow_delta.test.matchers import MatchesSendDeltaItemInvocation

from spreadflow_delta.proc import SetComputedValue

class SetComputedValueTestCase(TestCase):

    def test_computedvalue(self):
        """
        Test the computed value processor.
        """

        func = lambda key, doc: doc['test_content'] + " it's all organic"
        sut = SetComputedValue('test_result', func)
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'test_content': "Don't panic",
                }
            }
        }

        expected = copy.deepcopy(insert)
        expected['data']['a']['test_result'] = "Don't panic it's all organic"
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)

        sut(insert, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)
