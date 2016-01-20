from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy
import string
from spreadflow_delta.proc import Filter
from twisted.trial import unittest

from spreadflow_delta.test.util import SendMock

class FilterTestCase(unittest.TestCase):

    def sendmock(self, item, port):
        return SendMock(item, port, self)

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
        expected = copy.deepcopy(insert)
        send = self.sendmock(expected, sut)
        sut(insert, send)
        send.verify()

        delete = {
            'inserts': [],
            'deletes': ['a'],
            'data': {}
        }
        expected = copy.deepcopy(delete)
        send = self.sendmock(expected, sut)
        sut(delete, send)
        send.verify()


    def test_default_filter_anything(self):
        """
        Anything filtered, resulting message is expected to be empty.
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
        expected = {
            'inserts': [],
            'deletes': [],
            'data': {}
        }
        send = self.sendmock(expected, sut)
        sut(insert, send)
        send.verify()

        delete = {
            'inserts': [],
            'deletes': ['a'],
            'data': {}
        }
        expected = {
            'inserts': [],
            'deletes': [],
            'data': {}
        }
        send = self.sendmock(expected, sut)
        sut(delete, send)
        send.verify()
