from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

class SendMock(object):

    def __init__(self, item, port, testcase):
        self._expect_item = item
        self._expect_port = port
        self._invoked = 0
        self.testcase = testcase

    def __call__(self, item, port):
        self._invoked += 1
        self.testcase.assertEqual(port, self._expect_port)
        self.testcase.assertEqual(item.keys(), self._expect_item.keys())
        for key in item.keys():
            if key in {'inserts', 'deletes'}:
                self.testcase.assertEqual(set(item[key]), set(self._expect_item[key]))
            else:
                self.testcase.assertEqual(item[key], self._expect_item[key])

    def verify(self, invocations=1):
        self.testcase.assertEqual(self._invoked, invocations)
