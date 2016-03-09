from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy
import functools
import string

from mock import Mock
from testtools import TestCase
from twisted.internet import defer

from spreadflow_core.scheduler import Scheduler
from spreadflow_delta.test.matchers import MatchesSendDeltaItemInvocation

from spreadflow_delta.proc import MapReduce


class MapReduceTestCase(TestCase):

    @defer.inlineCallbacks
    def test_default_map_identity(self):
        """
        Perform identity transformation if no mapper is specified.
        """
        sut = MapReduce()
        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'b': 'c'
                }
            }
        }
        matches = MatchesSendDeltaItemInvocation(copy.deepcopy(insert), sut)
        send = Mock(spec=Scheduler.send)
        yield sut(insert, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        delete = {
            'inserts': [],
            'deletes': ['a'],
            'data': {}
        }
        matches = MatchesSendDeltaItemInvocation(copy.deepcopy(delete), sut)
        send = Mock(spec=Scheduler.send)
        yield sut(delete, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)


    @defer.inlineCallbacks
    def test_default_reduce_merge(self):
        """
        Merge updates to the same document if no reducer is specified.
        """
        def map(key, doc):
            # Unchanged document.
            yield key, doc
            # Subsequent addition to document.
            yield key, {'f': 'g'}
            # Subsequent modification of an existing key.
            yield key, {'b': 'x'}

        sut = MapReduce(map=map)
        msg = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'b': 'c',
                    'd': 'e'
                }
            }
        }
        expected = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'b': 'x',
                    'd': 'e',
                    'f': 'g'
                }
            }
        }
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        yield sut(msg, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)


    @defer.inlineCallbacks
    def test_filter(self):
        """
        Test filtering of documents by a mapper function.
        """
        def map(key, doc):
            if doc.get('allowed'):
                yield key, doc

        sut = MapReduce(map=map)
        insert = {
            'inserts': ['a', 'd'],
            'deletes': [],
            'data': {
                'a': {
                    'b': 'c',
                    'allowed': True
                },
                'd': {
                    'e': 'f',
                }
            }
        }
        expected = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'b': 'c',
                    'allowed': True
                },
            }
        }
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        yield sut(insert, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        insert = {
            'inserts': ['g'],
            'deletes': [],
            'data': {
                'g': {
                    'h': 'i',
                    'allowed': True
                }
            }
        }
        matches = MatchesSendDeltaItemInvocation(copy.deepcopy(insert), sut)
        send = Mock(spec=Scheduler.send)
        yield sut(insert, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        # Ensure that the filter is also effective when existing documents are
        # deleted.
        delete = {
            'inserts': [],
            'deletes': ['a', 'd', 'g'],
            'data': {}
        }
        expected = {
            'inserts': [],
            'deletes': ['a', 'g'],
            'data': {}
        }
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        yield sut(delete, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

    @defer.inlineCallbacks
    def test_term_frequency_with_update(self):
        """
        Test example implementation of term frequency analysis.
        """
        strip_punctuation = {ord(c): None for c in string.punctuation}
        def map(key, doc):
            for word in doc.translate(strip_punctuation).split():
                yield word, 1

        def reduce(key, values):
            return sum(values)

        sut = MapReduce(map=map, reduce=reduce)

        insert = {
            'inserts': ['line-1', 'line-2', 'line-3', 'line-4', 'line-5'],
            'deletes': [],
            'data': {
                'line-1': 'There was a fisherman named Fisher',
                'line-2': 'who fished for some fish in a fissure.',
                'line-3': 'Till a fish with a grin,',
                'line-4': 'pulled the firefighter in.',
                'line-5': 'Now they\'re fishing the fissure for Fisher.'
            }
        }

        expected_freq = {
            'Fisher': 2, 'Now': 1, 'There': 1, 'Till': 1, 'a': 4, 'fish': 2,
            'fished': 1, 'fisherman': 1, 'firefighter': 1, 'fishing': 1,
            'fissure': 2, 'for': 2, 'grin': 1, 'in': 2, 'named': 1,
            'pulled': 1, 'some': 1, 'the': 2, 'theyre': 1, 'was': 1, 'who': 1,
            'with': 1
        }
        expected = {
            'inserts': expected_freq.keys(),
            'deletes': [],
            'data': expected_freq
        }
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        yield sut(insert, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        update = {
            'inserts': ['line-4'],
            'deletes': ['line-4'],
            'data': {
                'line-4': 'pulled the fisherman in.',
            }
        }

        expected_freq = {
            'pulled': 1, 'the': 2, 'fisherman': 2, 'in': 2
        }
        expected = {
            'inserts': expected_freq.keys(),
            'deletes': ['firefighter'] + list(expected_freq.keys()),
            'data': expected_freq
        }
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        yield sut(update, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

    @defer.inlineCallbacks
    def test_reverse_dependency_with_update(self):
        """
        Test example implementation of reverse dependency analysis.
        """
        def map(subj, deplist):
            for dep in deplist:
                yield dep, [subj]

        def concat_list(key, values):
            return functools.reduce(lambda a, b: a + b, values, [])

        sut = MapReduce(map=map, reduce=concat_list)

        insert = {
            'inserts': ['stuff.c', 'util.c', 'other.c'],
            'deletes': [],
            'data': {
                'stuff.c': ['common.h', 'stuff.h'],
                'util.c': ['common.h', 'util.h'],
                'other.c': ['util.h']
            }
        }

        expected_rdep = {
            'common.h': ['stuff.c', 'util.c'],
            'stuff.h': ['stuff.c'],
            'util.h': ['util.c', 'other.c']
        }
        expected = {
            'inserts': expected_rdep.keys(),
            'deletes': [],
            'data': expected_rdep
        }
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        yield sut(insert, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)

        # Remove common.h from util.c
        update = {
            'inserts': ['util.c'],
            'deletes': ['util.c'],
            'data': {
                'util.c': ['util.h'],
            }
        }

        expected_rdep = {
            'common.h': ['stuff.c'],
            'util.h': ['other.c', 'util.c']
        }
        expected = {
            'inserts': expected_rdep.keys(),
            'deletes': expected_rdep.keys(),
            'data': expected_rdep
        }
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        yield sut(update, send)
        self.assertEquals(send.call_count, 1)
        self.assertThat(send.call_args, matches)
