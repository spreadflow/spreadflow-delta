from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy

from mock import Mock
from testtools import TestCase

from spreadflow_core.scheduler import Scheduler
from spreadflow_delta.test.matchers import MatchesSendDeltaItemInvocation

from spreadflow_delta.proc import Extractor

class ExtractorTestCase(TestCase):

    def test_simple_extractor(self):
        """
        A simple extractor copying the lowercase value of a string key.
        """

        def _lower(key, doc):
            doc['lower'] = doc['orig'].lower()

        sut = Extractor(_lower)
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'orig': 'sOmEsTr1nG!'
                }
            }
        }
        expected = copy.deepcopy(insert)
        expected['data']['a']['lower'] = 'somestr1ng!'
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        sut(insert, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

    def test_parametrized_extractor(self):
        """
        An extractor accepting parameters.
        """
        def _append(key, doc, suffix, resultkey='result', prefix='pfx'):
            doc[resultkey] = prefix + doc['orig'] + suffix

        sut = Extractor(_append, ' mySFX', prefix='realPFX ')
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'orig': 'sOmEsTr1nG!'
                }
            }
        }
        expected = copy.deepcopy(insert)
        expected['data']['a']['result'] = 'realPFX sOmEsTr1nG! mySFX'
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        sut(insert, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)
