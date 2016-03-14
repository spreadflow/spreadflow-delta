# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy

from mock import Mock, mock_open, patch, call
from testtools import TestCase, ExpectedException

from spreadflow_delta import util

class OpenReplaceTestCase(TestCase):

    def test_open_replace_write_ok(self):
        """
        Test the open_replace utility function (happy path).
        """

        open_mock = mock_open()
        open_mock.return_value.name = '/path/to/some/tmpXYZ213'

        with patch('spreadflow_delta.util.EncodedTemporaryFile', open_mock), patch('os.unlink') as unlink_mock, patch('os.rename') as rename_mock:
            with util.open_replace('/path/to/some/file.txt', encoding='iso-8859-1') as stream_mock:
                stream_mock.write('stuff')

        open_mock.assert_called_once_with(encoding='iso-8859-1', dir='/path/to/some', delete=False)
        self.assertFalse(unlink_mock.called)
        rename_mock.assert_called_once_with('/path/to/some/tmpXYZ213', '/path/to/some/file.txt')

    def test_open_replace_write_fail(self):
        """
        Test the open_replace utility function (exception during write).
        """

        open_mock = mock_open()
        open_mock.return_value.name = '/path/to/some/tmpXYZ213'

        with patch('spreadflow_delta.util.EncodedTemporaryFile', open_mock), patch('os.unlink') as unlink_mock, patch('os.rename') as rename_mock:
            with ExpectedException(RuntimeError, 'boom!'):
                with util.open_replace('/path/to/some/file.txt'):
                    raise RuntimeError('boom!')

        open_mock.assert_called_once_with(encoding=None, dir='/path/to/some', delete=False)
        unlink_mock.assert_called_once_with('/path/to/some/tmpXYZ213')
        self.assertFalse(rename_mock.called)
