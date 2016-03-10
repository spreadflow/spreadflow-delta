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

    SORT_DEFAULT = {
        'key': lambda triple: triple[0]
    }

    def _dummy_coiterate(self, iterator):
        for x in iterator:
            continue

        return defer.succeed(iterator)

    def __init__(self, coiterate=None):
        self.sort = self.SORT_DEFAULT

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


class MapReduce(MapReduceBase):

    def __init__(self, map=None, reduce=None, finalize=None, sort=None, coiterate=None):
        super(MapReduce, self).__init__(coiterate=coiterate)
        self._map_func = map if map else super(MapReduce, self).map
        self._reduce_func = reduce if reduce else super(MapReduce, self).reduce
        self._finalize_func = finalize if finalize else super(MapReduce, self).finalize
        self.sort = sort if sort else self.SORT_DEFAULT

    def map(self, key, value):
        for item in self._map_func(key, value):
            yield item

    def reduce(self, key, values):
        return self._reduce_func(key, values)


    def finalize(self, key, value):
        return self._finalize_func(key, value)


class UnpackBase(object):
    def __init__(self):
        self._subdocs = {}

    def __call__(self, item, send):
        if is_delta(item):
            unpacked_deletes = []
            unpacked_inserts = []
            unpacked_data = {}

            for oid in set(item['deletes'] + item['inserts']):
                for suboid in self._subdocs.pop(oid, []):
                    unpacked_deletes.append((oid, suboid))

            for oid in item['inserts']:
                self._subdocs[oid] = []
                for suboid, subdoc in self.unpack(oid, item['data'][oid]):
                    unpacked_data[(oid, suboid)] = subdoc
                    self._subdocs[oid].append(suboid)

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
        Generate subdocuments from the incoming insertable document.
        """
        raise NotImplementedError("Subclasses must implement this method")


class UnpackMapping(UnpackBase):
    def __init__(self, keys=None):
        super(UnpackMapping, self).__init__()
        self.keys = keys

    def unpack(self, oid, doc):
        for key, value  in doc.items():
            if self.keys is None or key in self.keys:
                yield key, value


class UnpackSequence(UnpackBase):
    def __init__(self, start=None, stop=None, step=None):
        super(UnpackSequence, self).__init__()
        self.start = start
        self.stop = stop
        self.step = step

    def unpack(self, oid, doc):
        return enumerate(doc[self.start:self.stop:self.step])


class Repack(object):
    def __call__(self, item, send):
        if is_delta(item):
            packed_item = item.pop('parent')
            for oid, suboid in sorted(item['deletes'], reverse=True):
                del packed_item['data'][oid][suboid]

            for oid, suboid in sorted(item['inserts']):
                doc = packed_item['data'][oid]
                subdoc = item['data'][(oid, suboid)]
                for key, value in self.pack(suboid, subdoc, oid, doc):
                    doc[key] = value

            send(packed_item, self)
        else:
            send(item, self)

    def pack(self, suboid, subdoc, oid, doc):
        yield suboid, subdoc


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
        tmpdir = os.path.dirname(path)

        stream = util.EncodedTemporaryFile(encoding=self.encoding, dir=tmpdir, delete=False)
        try:
            with stream:
                stream.write(doc[self.key])
        except:
            os.unlink(stream.name)
            raise
        else:
            os.rename(stream.name, path)

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
    def __init__(self, key='lockpath'):
        # oid -> lock.
        self._acquired = {}

        # list of (oid, doc).
        self._pending = []

        self.key = key
        self.out = object()
        self.out_retry = object()
        self.out_locked = object()

    def __call__(self, item, send):
        data = {}
        pending = []

        # Try to acquire a lockfile for all pending and new inserts.
        for oid, doc in self._inserts(item):
            try:
                self._acquired[oid] = _Lockfile.open(doc[self.key])
                data[oid] = doc
            except LockError:
                pending.append((oid, doc))

        # Reconstruct and send the message to the locked output port.
        pending_oids = set(oid for oid, doc in self._pending)
        item['deletes'] = [oid for oid in item['deletes'] if oid not in pending_oids]
        item['inserts'] = list(data.keys())
        item['data'] = data

        if not is_delta_empty(item):
            send(item, self.out_locked)

        # Push to retry output port.
        self._pending = pending
        if len(self._pending) > 0:
            retry_item = {
                'date': datetime.datetime.now(),
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
        self._pending = []

    def _inserts(self, item):
        """
        Iterates over pending and new inserts.
        """
        for oid, doc in self._pending:
            if oid not in item['deletes']:
                yield (oid, doc)

        for oid in item['inserts']:
            doc = item['data'][oid]
            yield (oid, doc)

    @property
    def dependencies(self):
        yield (self, self.out_locked)
        yield (self, self.out_retry)
        yield (self.release, self.out)
