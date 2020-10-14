import os
import pytest
import urllib

from ..locator import locator_factory, ResourceLocator, FileLocator, BaseLocator
from typing import Dict

@pytest.fixture()
def file_resource_url() -> str:
  resource_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_resource.json')
  resource_url = f'file://{resource_path}'
  yield resource_url  

@pytest.fixture()
def file_resource(file_resource_url) -> str:
  yield b'{"resource_property": "value"}'

def test_locator_parameters():
  url = 'https://user:pass@domain.com/path/components?q1=x&q2=y#fragment'
  parameters = {
    'encoding': 'utf-8',
  }
  url_with_locator_parameters = ResourceLocator.append_locator_parameters(url=url, parameters=parameters)
  assert url_with_locator_parameters == 'https://user:pass@domain.com/path/components?q1=x&q2=y&locator=1&encoding=utf-8#fragment'
  url_without_locator_parameters, stripped_parameters = ResourceLocator.strip_locator_parameters(url=url_with_locator_parameters)
  assert url_without_locator_parameters == url
  assert stripped_parameters == parameters

def test_file_locator(file_resource_url, file_resource):
  locator = locator_factory(url=file_resource_url)
  assert isinstance(locator, FileLocator)
  resource = locator.get()
  assert resource == file_resource

def test_file_locator_with_path(file_resource_url, file_resource):
  file_resource_path = urllib.parse.urlparse(file_resource_url).path
  locator = locator_factory(url=file_resource_path)
  assert isinstance(locator, FileLocator)
  resource = locator.get()
  assert resource == file_resource

def test_base_locator(file_resource_url, file_resource):
  locator = BaseLocator(url=file_resource_url)
  resource = locator.get()
  assert resource == file_resource

def test_alias(file_resource_url):
  ResourceLocator.register_url(
    alias='a',
    url=file_resource_url
  )
  assert ResourceLocator.get_registered_url(alias='a') == file_resource_url

def test_dealiasing():
  ResourceLocator.register_url(
    alias='a',
    url='https://example.com/b/c/?x=1&y=2&locator=1&encrypt=testa'
  )
  ResourceLocator.register_url(
    alias='b',
    url='alias://a/d.json?z=3&locator=1&encrypt=testb'
  )
  dealiased_url = ResourceLocator.dealias_url('alias://b')
  assert dealiased_url == 'https://example.com/b/c/d.json?x=1&y=2&z=3&locator=1&encrypt=testb'

