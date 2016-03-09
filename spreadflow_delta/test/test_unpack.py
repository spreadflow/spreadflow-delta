from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy

from mock import Mock
from testtools import TestCase

from spreadflow_core.scheduler import Scheduler
from spreadflow_delta.test.matchers import MatchesSendDeltaItemInvocation

from spreadflow_delta.proc import UnpackMapping, UnpackSequence, Repack

class UnpackTestCase(TestCase):

    def test_unpack_repack(self):
        unpackmap = UnpackMapping(keys=['m'])
        unpacklist = UnpackSequence()
        repacklist = Repack()
        repackmap = Repack()

        insert_orig = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'm': ['a', 'b', 'c'],
                },
            }
        }

        insert_expected_map = {
            'inserts': [('a', 'm')],
            'deletes': [],
            'data': {
                ('a', 'm'): ['a', 'b', 'c'],
            },
            'parent': insert_orig,
        }

        insert_expected_list = {
            'inserts': [
                (('a', 'm'), 0),
                (('a', 'm'), 1),
                (('a', 'm'), 2),
            ],
            'deletes': [],
            'data': {
                (('a', 'm'), 0): 'a',
                (('a', 'm'), 1): 'b',
                (('a', 'm'), 2): 'c',
            },
            'parent': insert_expected_map,
        }

        matches_unpack_map = MatchesSendDeltaItemInvocation(copy.deepcopy(insert_expected_map), unpackmap)
        matches_unpack_list = MatchesSendDeltaItemInvocation(copy.deepcopy(insert_expected_list), unpacklist)
        matches_repack_map = MatchesSendDeltaItemInvocation(copy.deepcopy(insert_expected_map), repacklist)
        matches_repack_orig = MatchesSendDeltaItemInvocation(copy.deepcopy(insert_orig), repackmap)

        send = Mock(spec=Scheduler.send)

        item = copy.deepcopy(insert_orig)

        unpackmap(item, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches_unpack_map)
        item = send.call_args[0][0]

        send = Mock(spec=Scheduler.send)

        unpacklist(item, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches_unpack_list)
        item = send.call_args[0][0]

        send = Mock(spec=Scheduler.send)

        repacklist(item, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches_repack_map)
        item = send.call_args[0][0]

        send = Mock(spec=Scheduler.send)

        repackmap(item, send)

        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches_repack_orig)
