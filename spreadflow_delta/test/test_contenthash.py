# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy

from mock import Mock
from testtools import TestCase

from spreadflow_core.scheduler import Scheduler
from spreadflow_delta.test.matchers import MatchesSendDeltaItemInvocation

from spreadflow_delta.proc import ContentHash

class ContentHashTestCase(TestCase):

    def test_contenthash(self):
        """
        Test the content hash processor.
        """

        sut = ContentHash(key='test_content', destkey='test_content_hash')
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'test_content': "Don't panic",
                }
            }
        }

        # >>> hashlib.sha1("Don't panic").hexdigest()
        # '8c731bcd5bff93c792107c113d64ce25fee8add0'

        expected = copy.deepcopy(insert)
        expected['data']['a']['test_content_hash'] = '8c731bcd5bff93c792107c113d64ce25fee8add0'
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)

        sut(insert, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)
