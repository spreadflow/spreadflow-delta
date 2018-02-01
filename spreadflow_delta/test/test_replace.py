from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy

from mock import Mock
from testtools import TestCase

from spreadflow_core.scheduler import Scheduler
from spreadflow_delta.test.matchers import MatchesSendDeltaItemInvocation

from spreadflow_delta.proc import Replace

class ReplaceTestCase(TestCase):

    def test_simple_replace(self):
        """
        A simple replace copying the lowercase value of a string key.
        """

        def _lower(doc, key):
            return doc.lower()

        sut = Replace(_lower)
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': 'sOmEsTr1nG!'
            }
        }
        expected = copy.deepcopy(insert)
        expected['data']['a'] = 'somestr1ng!'
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        sut(insert, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

    def test_parametrized_replace(self):
        """
        A replace accepting parameters.
        """
        def _append(doc, key, suffix, prefix='pfx'):
            return prefix + doc + suffix

        sut = Replace(_append, ' mySFX', prefix='realPFX ')
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': 'sOmEsTr1nG!'
            }
        }
        expected = copy.deepcopy(insert)
        expected['data']['a'] = 'realPFX sOmEsTr1nG! mySFX'
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        sut(insert, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

    def test_method_replace(self):
        """
        A replace test with methods instead of functions.
        """

        class B(object):
            pass

        b = B()

        class A(object):
            def toB(self, key):
                return b

        sut = Replace(A.toB)
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': A()
            }
        }
        expected = copy.deepcopy(insert)
        expected['data']['a'] = b
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        sut(insert, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)
