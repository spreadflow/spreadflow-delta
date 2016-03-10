from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import codecs
import os
import tempfile
import uuid

def EncodedTemporaryFile(encoding=None, *args, **kwds):
    tmp = tempfile.NamedTemporaryFile(*args, **kwds)

    if encoding is None:
        stream = tmp
    else:
        # @see codecs.open
        info = codecs.lookup(encoding)
        stream = codecs.StreamReaderWriter(tmp, info.streamreader, info.streamwriter)
        # Add attributes to simplify introspection
        stream.encoding = encoding
        stream.name = tmp.name

    return stream

def symlink_replace(src, dst):
    tmppath = tempfile.mkdtemp(dir=os.path.dirname(dst))
    tmplink = os.path.join(tmppath, str(uuid.uuid4()))
    os.symlink(src, tmplink)
    os.rename(tmplink, dst)
    os.rmdir(tmppath)
