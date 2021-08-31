import tempfile
import zipfile

from contextlib import contextmanager

@contextmanager
def decompress(method, stream):
  if method == 'none':
    with stream as s:
      yield s

  elif method == 'zip':
    # unfortunately it's hard to stream a zipfile since
    # it puts it's directory table at the end. Instead
    # we cache to the filesystem first
    with tempfile.NamedTemporaryFile() as fp:
      fp.write(stream.read())
      fp.seek(0)
      with zipfile.ZipFile(fp.name, 'r') as zip_ref:
        name = zip_ref.namelist()[0] # TODO: how to support multiple files?
        with zip_ref.open(name) as f:
          yield f
