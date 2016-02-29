from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import Mapping, Iterable

from twisted.internet import defer


def is_delta(item):
    """
    Returns True if the given item looks like a delta message.
    """

    if not isinstance(item, Mapping):
        return False

    if not 'inserts' in item or not isinstance(item['inserts'], Iterable):
        return False

    if not 'deletes' in item or not isinstance(item['deletes'], Iterable):
        return False

    if not 'data' in item or not isinstance(item['data'], Mapping):
        return False

    return True


def is_delta_empty(item):
    return len(item['inserts']) == 0 and len(item['deletes']) == 0


class Extractor(object):

    def __init__(self, extract=None):
        self._extract_func = extract

    def __call__(self, item, send):
        if is_delta(item) and not is_delta_empty(item):
            self._extract(item)

        send(item, self)

    def _extract(self, item):
        """
        Apply the extractor function on every insert.
        """
        for oid in item['inserts']:
            self.extract(oid, item['data'][oid])

    def extract(self, key, doc):
        self._extract_func(key, doc)


class Filter(object):

    def __init__(self, filter):
        self.filter = filter
        self._keys = set()

    def __call__(self, item, send):
        if is_delta(item) and not is_delta_empty(item):
            self._filter(item)
            if not is_delta_empty(item):
                send(item, self)

    def _filter(self, msg):
        """
        Applies the filter to a delta item.
        """
        orig_deletes = msg.pop('deletes')
        orig_inserts = msg.pop('inserts')
        orig_data = msg.pop('data')

        # Compute set of effectively deleted keys.
        keys_deleted = set(orig_deletes).intersection(self._keys)

        # Filter inserted data.
        data = {oid: orig_data[oid] for oid in orig_inserts if self.filter(oid, orig_data[oid])}

        # List of keys effectively processed.
        keys_inserted = set(data.keys())

        # Also send delete-events for keys which are modified by this message.
        keys_affected = keys_deleted.union(self._keys.intersection(keys_inserted))
        self._keys.difference_update(keys_affected - keys_inserted)
        self._keys.update(keys_inserted - keys_affected)

        # Reconstruct the message.
        msg['deletes'] = list(keys_affected)
        msg['inserts'] = list(keys_inserted)
        msg['data'] = data


class MapReduce(object):

    @staticmethod
    def map_default(key, value):
        yield key, value


    @staticmethod
    def map_chain(*map_funcs):
        def map_func(key, value):
            for f in map_funcs:
                for i in f(key, value):
                    yield i

        return map_func


    @staticmethod
    def reduce_default(key, values):
        return {k: v for doc in values for k, v in doc.items()}


    @staticmethod
    def finalize_default(key, value):
        return value


    SORT_DEFAULT = {
        'key': lambda triple: triple[0]
    }

    def _dummy_coiterate(self, iterator):
        for x in iterator:
            continue

        return defer.succeed(iterator)

    def __init__(self, map=None, reduce=None, finalize=None, sort=None, coiterate=None):
        self.map = map if map else self.map_default
        self.reduce = reduce if reduce else self.reduce_default
        self.finalize = finalize if finalize else self.finalize_default
        self.sort = sort if sort else self.SORT_DEFAULT

        if coiterate == True:
            from twisted.internet.task import coiterate

        if coiterate:
            self.coiterate = coiterate
        else:
            self.coiterate = self._dummy_coiterate

        self._triples = []
        self._keys = set()

    @defer.inlineCallbacks
    def __call__(self, item, send):
        if is_delta(item) and not is_delta_empty(item):
            yield self.coiterate(self._mapreduce(item))
            if not is_delta_empty(item):
                send(item, self)

    def _mapreduce(self, msg):
        # Remove triples for which we received a delete command.
        keys_deleted = self._delete(msg.pop('deletes'))

        # Compute all triples from the insert-set by applying the map function
        # and store them.
        keys_mapped = set()
        orig_data = msg.pop('data')
        for oid in msg.pop('inserts'):
            # FIXME: Maybe defer to another thread and yield
            tuples = self._map_one(oid, orig_data[oid])
            keys_mapped.update(self._add(oid, tuples))

        # Compute the list of triples which will be used in the subsequent
        # reduction step.
        triples = [t for t in self._triples if t[0] in keys_mapped.union(keys_deleted)]
        triples.sort(**self.sort)

        # Partition the data-set and apply the reduce function.
        buffer = {}
        for key, values in self._generate_partitions(triples):
            try:
                values = buffer[key] + values
            except KeyError:
                pass

            buffer[key] = [self._reduce_one(key, values)] if len(values) > 1 else values
            yield

        # Finalize reduced values and construct the data dictionary.
        data = {key: self.finalize(key, values[0]) for key, values in buffer.items()}

        # List of keys effectively processed.
        keys_inserted = set(data.keys())

        # Also send delete-events for keys which are modified by this message.
        keys_affected = keys_deleted.union(self._keys.intersection(keys_inserted))
        self._keys.difference_update(keys_affected - keys_inserted)
        self._keys.update(keys_inserted - keys_affected)

        # Reconstruct the message.
        msg['deletes'] = list(keys_affected)
        msg['inserts'] = list(keys_inserted)
        msg['data'] = data


    def _map_one(self, key, value):
        return list(self.map(key, value))


    def _reduce_one(self, key, values):
        return self.reduce(key, values)


    def _generate_partitions(self, triples):
        if len(triples) > 0:
            current_key, value, oid_ignored = triples[0]
            current_values = [value]

            for key, value, oid_ignored in triples[1:]:
                if key != current_key:
                    if len(current_values) > 0:
                        yield current_key, current_values
                    current_key = key
                    current_values = []

                current_values.append(value)

            if len(current_values) > 0:
                yield current_key, current_values


    def _delete(self, oids):
        keys_deleted = set([t[0] for t in self._triples if t[2] in oids])
        self._triples = [t for t in self._triples if t[2] not in oids]
        return keys_deleted


    def _add(self, oid, tuples):
        columns = list(zip(*tuples)) + [[oid] * len(tuples)]
        keys_inserted = set(columns[0])
        self._triples += list(zip(*columns))
        return keys_inserted
