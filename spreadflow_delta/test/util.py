from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from spreadflow_delta.test.matcher import MatchesDeltaItem

class SendMock(object):

    def __init__(self, item, port, testcase):
        self._expect_item = item
        self._expect_port = port
        self._invoked = 0
        self.testcase = testcase

    def __call__(self, item, port):
        self._invoked += 1
        self.testcase.assertThat(item, MatchesDeltaItem(self._expect_item))
        self.testcase.assertEqual(port, self._expect_port)

    def verify(self, invocations=1):
        self.testcase.assertEqual(self._invoked, invocations)
