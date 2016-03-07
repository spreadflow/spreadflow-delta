# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy

from mock import Mock
from testtools import TestCase, ExpectedException

from spreadflow_core.scheduler import Scheduler
from spreadflow_delta.test.matchers import MatchesSendDeltaItemInvocation

from spreadflow_delta.proc import Fileurl

class FileurlTestCase(TestCase):

    def test_fileurl(self):
        """
        Test the fileurl processor.
        """

        sut = Fileurl(key='test_path', destkey='test_url', basedir='/path/to', baseurl='http://example.com/server/path/')
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'test_path': '/path/to/some/rEsourçe WITH fün chárs.txt',
                }
            }
        }
        expected = copy.deepcopy(insert)
        expected['data']['a']['test_url'] = 'http://example.com/server/path/some/rEsour%C3%A7e%20WITH%20f%C3%BCn%20ch%C3%A1rs.txt'
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)

        sut(insert, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

    def test_basedir_assertion(self):
        """
        Test the fileurl processor.
        """

        sut = Fileurl(key='test_path', destkey='test_url', basedir='/nowhere', baseurl='http://example.com/server/path/')
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'test_path': '/path/to/some/rEsourçe WITH fün chárs.txt',
                }
            }
        }

        send = Mock(spec=Scheduler.send)

        with ExpectedException(AssertionError, 'Cannot generate URLs for files outside of the basedir'):
            sut(insert, send)

        self.assertEquals(send.call_count, 0)
