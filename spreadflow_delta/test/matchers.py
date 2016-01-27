from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from testtools import matchers
from spreadflow_core.test.matchers import MatchesInvocation

class MatchesDeltaItem(matchers.MatchesDict):
    def __init__(self, item):
        super(MatchesDeltaItem, self).__init__({
            'data': matchers.Equals(item['data']),
            'inserts': matchers.MatchesSetwise(*[matchers.Equals(oid) for oid in item['inserts']]),
            'deletes': matchers.MatchesSetwise(*[matchers.Equals(oid) for oid in item['deletes']])
        })

class MatchesSendDeltaItemInvocation(MatchesInvocation):
    def __init__(self, expected_item, expected_port):
        super(MatchesSendDeltaItemInvocation, self).__init__(
            MatchesDeltaItem(expected_item),
            matchers.Equals(expected_port)
        )
