from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import codecs
import collections
import datetime
import errno
import hashlib
import os
import shutil
import tempfile

try:
    from urllib.parse import quote as urlquote
except ImportError:
    from urllib import quote as urlquote

from twisted.internet import defer

from spreadflow_core.component import Compound
from spreadflow_core.dsl.stream import AddTokenOp
from spreadflow_core.dsl.parser import ComponentParser
from spreadflow_core.dsl.tokens import \
    ComponentToken, \
    ConnectionToken, \
    DefaultInputToken, \
    DefaultOutputToken, \
    ParentElementToken
from spreadflow_core.proc import Throttle, Sleep
from spreadflow_core.script import ProcessTemplate, ChainTemplate
from spreadflow_delta import util


def is_delta(item):
    """
    Returns True if the given item looks like a delta message.
    """

    if not isinstance(item, collections.Mapping):
        return False

    if not 'inserts' in item or not isinstance(item['inserts'], collections.Iterable):
        return False

    if not 'deletes' in item or not isinstance(item['deletes'], collections.Iterable):
        return False

    if not 'data' in item or not isinstance(item['data'], collections.Mapping):
        return False

    return True


def is_delta_empty(item):
    return len(item['inserts']) == 0 and len(item['deletes']) == 0


class ReplaceBase(object):
    """
    Abstract base class for replacers.
    """

    def __call__(self, item, send):
        if is_delta(item) and not is_delta_empty(item):
            for oid in item['inserts']:
                item['data'][oid] = self.replace(item['data'][oid], oid)

        send(item, self)

    def replace(self, doc, key):
        """
        Replace the incoming insertable document.
        """
        raise NotImplementedError("Subclasses must implement this method")


class Replace(ReplaceBase):
    """
    Replace the incoming insertable document with a computed result.
    """

    def __init__(self, func, *args, **kwds):
        self._replace_func = func
        self._replace_args = args
        self._replace_kwds = kwds

    def replace(self, doc, key):
        return self._replace_func(doc, key, *self._replace_args, **self._replace_kwds)


class ReplaceKeyBase(object):
    """
    Abstract base class for key replacers.
    """

    def __call__(self, item, send):
        if is_delta(item) and not is_delta_empty(item):
            replacedInserts = [self.replace_key(oid) for oid in item['inserts']]
            replacedDeletes = [self.replace_key(oid) for oid in item['deletes']]
            replacedData = {repl: item['data'][oid] for oid, repl in zip(item['inserts'], replacedInserts)}

            item['inserts'] = replacedInserts
            item['deletes'] = replacedDeletes
            item['data'] = replacedData

        send(item, self)

    def replace_key(self, key):
        """
        Replace the incoming insertable document.
        """
        raise NotImplementedError("Subclasses must implement this method")


class ReplaceKey(ReplaceKeyBase):
    """
    Replace the incoming insertable and deletable keys with a computed result.
    """

    def __init__(self, func, *args, **kwds):
        self._replace_func = func
        self._replace_args = args
        self._replace_kwds = kwds

    def replace_key(self, key):
        return self._replace_func(key, *self._replace_args, **self._replace_kwds)


class ExtractorBase(object):
    """
    Abstract base class for extractors.
    """

    def __call__(self, item, send):
        if is_delta(item) and not is_delta_empty(item):
            for oid in item['inserts']:
                self.extract(oid, item['data'][oid])

        send(item, self)

    def extract(self, key, doc):
        """
        Change the incoming insertable document.
        """
        raise NotImplementedError("Subclasses must implement this method")


class Extractor(ExtractorBase):

    def __init__(self, func, *args, **kwds):
        self._extract_func = func
        self._extract_args = args
        self._extract_kwds = kwds

    def extract(self, key, doc):
        self._extract_func(key, doc, *self._extract_args, **self._extract_kwds)


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


class MapReduceBase(object):

    def map(self, key, value):
        yield key, value


    def reduce(self, key, values):
        return {k: v for doc in values for k, v in doc.items()}


    def finalize(self, key, value):
        return value


    def sort_key(self, key, value, dockey):
        return key

    sort_reverse = False


    def _dummy_coiterate(self, iterator):
        for x in iterator:
            continue

        return defer.succeed(iterator)

    def __init__(self, coiterate=None):
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
            if len(tuples):
                keys_mapped.update(self._add(oid, tuples))

        # Compute the list of triples which will be used in the subsequent
        # reduction step.
        triples = [t for t in self._triples if t[0] in keys_mapped.union(keys_deleted)]
        triples.sort(
            key=lambda triple: self.sort_key(*triple),
            reverse=self.sort_reverse
        )

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


class MapReduce(MapReduceBase):

    def __init__(self, map=None, reduce=None, finalize=None, sort_key=None, sort_reverse=False, coiterate=None):
        super(MapReduce, self).__init__(coiterate=coiterate)
        self._map_func = map if map else super(MapReduce, self).map
        self._reduce_func = reduce if reduce else super(MapReduce, self).reduce
        self._finalize_func = finalize if finalize else super(MapReduce, self).finalize
        self._sort_key_func = sort_key if sort_key else super(MapReduce, self).sort_key
        self.sort_reverse = sort_reverse

    def map(self, key, value):
        for item in self._map_func(key, value):
            yield item

    def reduce(self, key, values):
        return self._reduce_func(key, values)

    def finalize(self, key, value):
        return self._finalize_func(key, value)

    def sort_key(self, key, value, dockey):
        return self._sort_key_func(key, value, dockey)


class UnpackBase(object):
    def __init__(self, key):
        self._subdocs = {}
        self.key = key

    def __call__(self, item, send):
        if is_delta(item):
            unpacked_deletes = []
            unpacked_inserts = []
            unpacked_data = {}

            for oid in set(item['deletes'] + item['inserts']):
                for idx in self._subdocs.pop(oid, []):
                    unpacked_deletes.append((oid, self.key, idx))

            for oid in item['inserts']:
                self._subdocs[oid] = []
                for idx, subdoc in enumerate(self.unpack(oid, item['data'][oid])):
                    unpacked_data[(oid, self.key, idx)] = subdoc
                    self._subdocs[oid].append(idx)

            unpacked_item = {
                'inserts': list(unpacked_data.keys()),
                'deletes': unpacked_deletes,
                'data': unpacked_data,
                'parent': item
            }
            send(unpacked_item, self)

        else:
            send(item, self)

    def unpack(self, oid, doc):
        """
        Unpack a document into a list of subdocuments.
        """


class UnpackValue(UnpackBase):
    def unpack(self, oid, doc):
        yield doc[self.key]


class UnpackSequence(UnpackBase):
    def __init__(self, key, start=None, stop=None, step=None):
        super(UnpackSequence, self).__init__(key)
        self.start = start
        self.stop = stop
        self.step = step

    def unpack(self, oid, doc):
        return doc[self.key][self.start:self.stop:self.step]


class Unpack(UnpackBase):
    def __init__(self, key, func):
        super(Unpack, self).__init__(key)
        self.func = func

    def unpack(self, oid, doc):
        for item in self.func(oid, doc):
            yield item


class RepackBase(object):
    def __init__(self, destkey):
        self.destkey = destkey

    def __call__(self, item, send):
        if is_delta(item):
            packed_item = item.pop('parent')

            lists = collections.defaultdict(list)
            for subkey in sorted(item['inserts']):
                lists[subkey[0]].append(item['data'][subkey])

            for oid in packed_item['inserts']:
                packed_item['data'][oid][self.destkey] = self.pack(oid, lists[oid])

            send(packed_item, self)
        else:
            send(item, self)

    def pack(self, oid, values):
        """
        Repack values into the document identified by the oid.
        """
        raise NotImplementedError("Subclasses must implement this method")


class RepackValue(RepackBase):
    def __init__(self, destkey, default=None):
        super(RepackValue, self).__init__(destkey)
        self.default = default

    def pack(self, oid, values):
        try:
            result = values[0]
        except IndexError:
            result = self.default

        return result


class RepackSequence(RepackBase):
    def __init__(self, destkey, factory=list):
        super(RepackSequence, self).__init__(destkey)
        self.factory = factory

    def pack(self, oid, values):
        return self.factory(values)


class Repack(RepackBase):
    def __init__(self, destkey, func):
        super(Repack, self).__init__(destkey)
        self.func = func

    def pack(self, oid, values):
        return self.func(values)


class SetComputedValue(ExtractorBase):
    def __init__(self, destkey, func):
        self.destkey = destkey
        self.func = func

    def extract(self, key, doc):
        doc[self.destkey] = self.func(key, doc)


class Loadfile(ExtractorBase):
    def __init__(self, key='path', destkey='content', encoding='utf-8'):
        self.key = key
        self.destkey = destkey
        self.encoding = encoding

    def extract(self, key, doc):
        path = doc[self.key]
        with codecs.open(path, encoding=self.encoding, mode='r') as stream:
            doc[self.destkey] = stream.read()


class Savefile(ExtractorBase):
    def __init__(self, key='content', destkey='savepath', encoding='utf-8', clear=False):
        self.key = key
        self.destkey = destkey
        self.encoding = encoding
        self.clear = clear

    def extract(self, key, doc):
        path = doc[self.destkey]

        with util.open_replace(path, encoding=self.encoding) as stream:
            stream.write(doc[self.key])

        if self.clear:
            del doc[self.key]


class Symlink(ExtractorBase):
    def __init__(self, key='path', destkey='linkpath'):
        self.key = key
        self.destkey = destkey

    def extract(self, key, doc):
        path = doc[self.key]
        linkpath = doc[self.destkey]

        util.symlink_replace(path, linkpath)


class Fileurl(ExtractorBase):
    def __init__(self, key='savepath', destkey='content_url', basedir='', baseurl=''):
        self.key = key
        self.destkey = destkey
        self.basedir = basedir
        self.baseurl = baseurl

    def extract(self, key, doc):
        assert doc[self.key].startswith(self.basedir), 'Cannot generate URLs for files outside of the basedir'
        relpath = doc[self.key][len(self.basedir):].lstrip('/')
        doc[self.destkey] = self.baseurl + urlquote(relpath.encode('utf-8'))


class ContentHash(ExtractorBase):
    def __init__(self, key='content', destkey='content_hash', encoding='utf-8', hashalgo='sha1', hashseed=None):
        self.key = key
        self.destkey = destkey
        self.encoding = encoding
        self.hashalgo = hashalgo
        self.hashseed = hashseed
        self.hashobj = None

    def extract(self, key, doc):
        if self.hashobj is None:
            self.hashobj = hashlib.new(self.hashalgo)
            if self.hashseed is not None:
                self.hashobj.update(self.hashseed)

        content = doc[self.key]
        if self.encoding is not None:
            content = content.encode('utf-8')

        doc[self.destkey] = hashlib.sha1(content).hexdigest()


class Cachedir(object):
    def __init__(self, directory=None, destkey='cachedir', hashalgo='sha1', hashseed=None, clean=True):
        self.directory = directory
        self.destkey = destkey
        self.hashalgo = hashalgo
        self.hashseed = hashseed
        self.clean = clean
        self.hashobj = None
        self.slicelen = 2
        self._dirclean = False

    def __call__(self, item, send):
        if is_delta(item):
            if self.directory is None:
                self.directory = tempfile.mkdtemp()
                self._dirclean = True

            oids = set(item['deletes'] + item['inserts'])
            pathmap = self._generate_pathmap(self.directory, oids)

            if self.clean:
                for oid, path in pathmap.items():
                    shutil.rmtree(path, ignore_errors=(oid in item['inserts']))

            for oid in item['inserts']:
                path = pathmap[oid]

                try:
                    os.makedirs(path)
                except OSError as err:
                    if self.clean or err.errno != errno.EEXIST:
                        raise

                item['data'][oid][self.destkey] = path

        send(item, self)

    def detach(self):
        if self._dirclean:
            shutil.rmtree(self.directory, ignore_errors=True)

    def _generate_pathmap(self, basedir, oids):
        result = {}

        if self.hashobj is None:
            self.hashobj = hashlib.new(self.hashalgo)
            if self.hashseed is not None:
                self.hashobj.update(self.hashseed)

        for oid in oids:
            hashobj = self.hashobj.copy()
            hashobj.update(str(oid).encode('utf-8'))
            digest = hashobj.hexdigest()
            outer, inner = digest[:self.slicelen], digest[self.slicelen:]
            result[oid] = os.path.join(basedir, outer, inner)

        return result


class LockError(RuntimeError):
    pass

class _Lockfile:
    """A separate object allowing proper closing of a temporary file's
    underlying file descriptor
    """

    fd = None
    close_called = False

    @classmethod
    def open(cls, lockpath):
        try:
            fd = os.open(lockpath, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            return cls(fd, lockpath)
        except OSError as err:
            if err.errno in [errno.EAGAIN, errno.EEXIST, errno.EDEADLK, errno.ENOENT]:
                raise LockError()
            else:
                raise

    # Cache the unlinker so we don't get spurious errors at
    # shutdown when the module-level "os" is None'd out.
    def close(self, os_unlink=os.unlink, os_close=os.close):
        if not self.close_called and self.fd is not None:
            self.close_called = True
            try:
                os_close(self.fd)
            finally:
                os_unlink(self.lockpath)

    def __init__(self, fd, lockpath):
        self.fd = fd
        self.lockpath = lockpath

    # Need to ensure the file is deleted on __del__
    def __del__(self):
        self.close()


class LockingProcessor(object):
    _now = datetime.datetime.now

    def __init__(self, key='lockpath'):
        # oid -> lock.
        self._acquired = {}

        # A delta item with all pending deletes/inserts
        self._pending = {}

        # A list of known lockpaths
        # oid -> path
        self._known = {}

        self.key = key
        self.out = object()
        self.out_retry = object()
        self.out_locked = object()

    def __call__(self, item, send):
        merged = self._merge(item)

        # Try to acquire a lockfile for all pending and new inserts.
        locked_data = {}
        pending_data = {}
        for oid, doc in merged['data'].items():
            try:
                self._acquired[oid] = _Lockfile.open(doc[self.key])
                locked_data[oid] = doc
            except LockError:
                pending_data[oid] = doc

        pending_paths = set([doc[self.key] for doc in pending_data.values()])
        pending_deletes = []
        locked_deletes = []
        for oid in merged['deletes']:
            # pass a delete to pending if a delete has a known path and there
            # is a pending insert for that path.
            if oid in self._known and self._known[oid] in pending_paths:
                pending_deletes.append(oid)
            else:
                locked_deletes.append(oid)

        # Reconstruct and send the message to the locked output port.
        item['deletes'] = list(locked_deletes)
        item['inserts'] = list(locked_data.keys())
        item['data'] = locked_data

        if not is_delta_empty(item):
            send(item, self.out_locked)

        # Update known paths.
        for oid in locked_deletes:
            self._known.pop(oid)

        for oid, doc in locked_data.items():
            self._known[oid] = doc[self.key]

        # Update pending.
        self._pending = {
            'deletes': pending_deletes,
            'inserts': list(pending_data.keys()),
            'data': pending_data
        }

        # Push to retry output port.
        if not is_delta_empty(self._pending):
            retry_item = {
                'date': self._now(),
                'deletes': (),
                'inserts': (),
                'data': {}
            }
            send(retry_item, self.out_retry)

    def release(self, item, send):
        """
        Release the locks and send the message to the default output port.
        """
        for oid in item['inserts']:
            self._acquired.pop(oid).close()

        send(item, self.out)

    def attach(self, dispatcher, reactor):
        pass

    def detach(self):
        for lockfile in self._acquired.values():
            lockfile.close()

        self._acquired = {}
        self._pending = {}
        self._known = {}

    def _merge(self, item):
        """
        Merges pending docs with the new item.
        """
        merged_data = {}
        merged_deletes = self._pending.get('deletes', [])

        pending_data = self._pending.get('data', {})
        for oid in self._pending.get('inserts', []):
            if oid not in item['deletes']:
                merged_data[oid] = pending_data[oid]

        for oid in item['deletes']:
            if oid not in self._pending.get('inserts', []):
                merged_deletes.append(oid)

        merged_data.update(item['data'])

        return {
            'deletes': merged_deletes,
            'inserts': list(merged_data.keys()),
            'data': merged_data
        }

    @property
    def ins(self):
        return [self, self.release]

    @property
    def outs(self):
        return [self.out_locked, self.out_retry, self.out]

class LockingProcessorTemplate(ProcessTemplate):
    key = 'lockpath'

    def __init__(self, key=None):
        if key is not None:
            self.key = key

    def apply(self):
        process = LockingProcessor(self.key)
        yield AddTokenOp(ComponentToken(process))

        yield AddTokenOp(ParentElementToken(process.out, process))
        yield AddTokenOp(ParentElementToken(process.out_locked, process))
        yield AddTokenOp(ParentElementToken(process.out_retry, process))
        yield AddTokenOp(ParentElementToken(process.release, process))
        yield AddTokenOp(DefaultOutputToken(process, process.out))

class LockedProcessTemplate(ProcessTemplate):
    chain = None
    delay = 5
    key = 'lockpath'
    component_parser = ComponentParser()

    def __init__(self, chain=None, delay=None, key=None):
        if chain is not None:
            self.chain = chain
        if delay is not None:
            self.delay = delay
        if key is not None:
            self.key = key

    def apply(self):
        stream = LockingProcessorTemplate(self.key).apply()
        for operation in self.component_parser.divert(stream):
            yield operation
        lock = self.component_parser.get_component()

        stream = self._locked_chain().apply()
        for operation in self.component_parser.divert(stream):
            yield operation
        locked_chain = self.component_parser.get_component()

        stream = self._retry_chain().apply()
        for operation in self.component_parser.divert(stream):
            yield operation
        retry_chain = self.component_parser.get_component()

        # Connect the locked chain.
        yield AddTokenOp(ConnectionToken(lock.out_locked, locked_chain))
        yield AddTokenOp(ConnectionToken(locked_chain, lock.release))

        # Connect the retry chain.
        yield AddTokenOp(ConnectionToken(lock.out_retry, retry_chain))
        yield AddTokenOp(ConnectionToken(retry_chain, lock))

        # Create a parent for all three components.
        process = Compound(children=[lock, locked_chain, retry_chain])
        yield AddTokenOp(ComponentToken(process))
        yield AddTokenOp(ParentElementToken(lock, process))
        yield AddTokenOp(ParentElementToken(locked_chain, process))
        yield AddTokenOp(ParentElementToken(retry_chain, process))

        # Add default input and default output ports.
        yield AddTokenOp(DefaultInputToken(process, lock))
        yield AddTokenOp(DefaultOutputToken(process, lock.out))

    def _locked_chain(self):
        return ChainTemplate(chain=self.chain)

    def _retry_chain(self):
        return ChainTemplate(chain=[
            Throttle(self.delay),
            Sleep(self.delay),
        ])
