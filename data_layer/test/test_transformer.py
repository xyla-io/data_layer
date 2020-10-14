import os
import pytest
import urllib

from ..transformer import ZipUntransformer

@pytest.fixture()
def zipfile_bytes() -> bytes:
  resource_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test.zip')
  with open(resource_path, mode='rb') as f:
    yield f.read()

def test_untransform_zipfile(zipfile_bytes: bytes):
  untransformer = ZipUntransformer(resource=zipfile_bytes)
  data = untransformer.untransform()
  assert data == {'test.key': b'Key Content', 'test.pem': b'Pem Content'}
