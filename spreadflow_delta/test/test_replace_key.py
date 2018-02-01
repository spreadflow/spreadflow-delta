from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy

from mock import Mock
from testtools import TestCase

from spreadflow_core.scheduler import Scheduler
from spreadflow_delta.test.matchers import MatchesSendDeltaItemInvocation

from spreadflow_delta.proc import ReplaceKey

class ReplaceKeyTestCase(TestCase):

    def test_simple_replace_key(self):
        """
        Swap caps of keys
        """

        def _swapcase(key):
            return key.swapcase()

        sut = ReplaceKey(_swapcase)
        insert = {
            'inserts': ['aKey'],
            'deletes': ['anOldOne'],
            'data': {
                'aKey': 'sOmEsTr1nG!'
            }
        }
        expected = {
            'inserts': ['AkEY'],
            'deletes': ['ANoLDoNE'],
            'data': {
                'AkEY': 'sOmEsTr1nG!'
            }
        }
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        sut(insert, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

    def test_parametrized_replace_key(self):
        """
        A replace accepting parameters.
        """
        def _append(key, suffix, prefix='pfx'):
            return prefix + key + suffix

        sut = ReplaceKey(_append, ' mySFX', prefix='realPFX ')
        insert = {
            'inserts': ['a'],
            'deletes': ['b'],
            'data': {
                'a': 'sOmEsTr1nG!'
            }
        }
        expected = {
            'inserts': ['realPFX a mySFX'],
            'deletes': ['realPFX b mySFX'],
            'data': {
                'realPFX a mySFX': 'sOmEsTr1nG!'
            }
        }
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        sut(insert, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

    def test_method_replace_key(self):
        """
        A replace test with methods instead of functions.
        """

        class B(object):
            pass

        b = B()

        class A(object):
            def toB(self):
                return b

        a = A()

        sut = ReplaceKey(A.toB)
        insert = {
            'inserts': [a],
            'deletes': [],
            'data': {
                a: 'sOmEsTr1nG!'
            }
        }
        expected = {
            'inserts': [b],
            'deletes': [],
            'data': {
                b: 'sOmEsTr1nG!'
            }
        }
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        sut(insert, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)
