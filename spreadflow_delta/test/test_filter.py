from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy

from mock import Mock
from testtools import TestCase

from spreadflow_core.scheduler import Scheduler
from spreadflow_delta.test.matchers import MatchesSendDeltaItemInvocation

from spreadflow_delta.proc import Filter

class FilterTestCase(TestCase):

    def test_default_filter_nothing(self):
        """
        Nothing filtered, message is expected to pass unaltered.
        """
        sut = Filter(lambda key, doc: True)
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'b': 'c'
                }
            }
        }
        matches = MatchesSendDeltaItemInvocation(copy.deepcopy(insert), sut)
        send = Mock(spec=Scheduler.send)
        sut(insert, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        delete = {
            'inserts': [],
            'deletes': ['a'],
            'data': {}
        }
        matches = MatchesSendDeltaItemInvocation(copy.deepcopy(delete), sut)
        send = Mock(spec=Scheduler.send)
        sut(delete, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)


    def test_default_filter_anything(self):
        """
        Anything filtered, nothing is forwarded to dowstream.
        """
        sut = Filter(lambda key, doc: False)
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'b': 'c'
                }
            }
        }
        send = Mock(spec=Scheduler.send)
        sut(insert, send)
        self.assertEquals(send.call_count, 0)

        delete = {
            'inserts': [],
            'deletes': ['a'],
            'data': {}
        }
        send = Mock(spec=Scheduler.send)
        sut(delete, send)
        self.assertEquals(send.call_count, 0)

        not_a_delta = "an arbitrary string message"
        send = Mock(spec=Scheduler.send)
        sut(not_a_delta, send)
        self.assertEquals(send.call_count, 0)
