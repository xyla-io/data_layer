from __future__ import annotations
import urllib
import shutil
import shlex
import os
import boto3
import botocore
import tempfile
import subprocess
import atexit

from pathlib import Path
from typing import Dict, Callable, Tuple, List, Optional
from .error import LocationError, LocationRegistryError, LocationRegistryCircularDependencyError
from .transformer import ZipUntransformer, EncryptTransformer, DecryptUntransformer, DecodeUntransformer, JSONUntransformer

def handle_location_error(f: Callable[[ResourceLocator, any], any]) -> Callable[[ResourceLocator, ...], any]:
  def wrapper(self, *args, **kwargs):
    try:
      return f(self, *args, **kwargs)
    except (KeyboardInterrupt, SystemExit):
      raise
    except Exception as e:
      raise LocationError(url=self.url, error=e)
  return wrapper

def transform_resource(f: Callable[[ResourceLocator, any], any]) -> Callable[[ResourceLocator, ...], any]:
  def wrapper(self, resource: Optional[any], *args, **kwargs):
    return f(
      self,
      resource=self.transform(resource=resource),
      *args,
      **kwargs
    )
  return wrapper

def untransform_resource(f: Callable[[ResourceLocator, any], any]) -> Callable[[ResourceLocator, ...], any]:
  def wrapper(self, *args, **kwargs):
    return self.untransform(
      resource=f(self, *args, **kwargs)
    )
  return wrapper

class ResourceLocator:
  safe: bool=True
  url: str
  registry: Dict[str, str]={}

  @classmethod
  def join_path(cls, url: str, path) -> str:
    parts = urllib.parse.urlparse(url)
    joined_url = urllib.parse.urlunparse([*parts[:2], urllib.parse.urljoin(parts.path, path), *parts[3:]])
    return joined_url

  @classmethod
  def append_locator_parameters(cls, url: str, parameters: Dict[str, any]) -> str:
    parts = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qsl(parts.query) + [('locator', '1')] + [(k, parameters[k]) for k in sorted(parameters.keys())]
    url_with_locator_parameters = urllib.parse.urlunparse([*parts[:4], urllib.parse.urlencode(query), *parts[5:]])
    return url_with_locator_parameters

  @classmethod
  def strip_locator_parameters(cls, url) -> Tuple[str, Dict[str, any]]:
    parts = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qsl(parts.query)
    try:
      locator_index = len(query) - 1 - query[::-1].index(('locator', '1'))
    except ValueError:
      return (url, {})
    parameters = {
      t[0]: t[1]
      for t in query[locator_index + 1:]
    }
    query = query[:locator_index]
    url_without_locator_parameters = urllib.parse.urlunparse([*parts[:4], urllib.parse.urlencode(query), *parts[5:]])
    return (url_without_locator_parameters, parameters)

  @classmethod
  def locator_flag_value(cls, raw_value: any) -> bool:
    return raw_value.lower() in ['1', 'true', 't', 'yes', 'y']

  @classmethod
  def merge_urls(cls, base_url: str, url: str) -> str:
    base, base_parameters = cls.strip_locator_parameters(url=base_url)
    to_merge, parameters = cls.strip_locator_parameters(url=url)
    base_parts = urllib.parse.urlparse(base)
    parts = urllib.parse.urlparse(to_merge)
    hostname = parts.hostname if parts.hostname is not None else base_parts.hostname
    username = parts.username if parts.username is not None else base_parts.username
    password = parts.password if parts.password is not None else base_parts.password
    netloc = hostname if hostname is not None else ''
    if username is not None:
      netloc = f'{f"{username}:{password}" if password is not None else username}@{netloc}'
    query = urllib.parse.parse_qsl(base_parts.query) + urllib.parse.parse_qsl(parts.query)
    merged_parts = [
      parts.scheme if parts.scheme else base_parts.scheme,
      netloc,
      urllib.parse.urljoin(base_parts.path, parts.path[1:] if base_parts.path else parts.path),
      parts.params if parts.params else base_parts.params,
      urllib.parse.urlencode(query),
      parts.fragment if parts.fragment else base_parts.fragment
    ]
    merged_url = urllib.parse.urlunparse(merged_parts)
    return cls.append_locator_parameters(
      url=merged_url,
      parameters={
        **base_parameters,
        **parameters,
      }
    )

  @classmethod
  def register_url(cls, alias: str, url: str):
    cls.registry[alias] = url

  @classmethod
  def get_registered_url(cls, alias: str) -> str:
    if alias not in cls.registry:
      raise LocationRegistryError(alias=alias)
    return cls.registry[alias]

  @classmethod
  def get_url_parts(cls, url: str) -> urllib.parse.ParseResult:
    return urllib.parse.urlparse(url)

  @classmethod
  def dealias_url(cls, url: str, _chain: Optional[List[str]]=None) -> str:
    if _chain is None:
      _chain = []

    parts = urllib.parse.urlparse(url)
    if parts.scheme != 'alias':
      return url
    alias = parts.hostname
    if alias in _chain:
      raise LocationRegistryCircularDependencyError(chain=[*_chain, alias])
    base_url = cls.get_registered_url(alias=alias)
    if parts.username is None:
      merge_netloc = ''
    elif parts.password is None:
      merge_netloc = f'{parts.username}@'
    else:
      merge_netloc = f'{parts.username}:{parts.password}@'
    url_to_merge = urllib.parse.urlunparse(['', merge_netloc, *parts[2:]])
    merged_url = cls.merge_urls(
      base_url=base_url,
      url=url_to_merge
    )
    return cls.dealias_url(
      url=merged_url,
      _chain=[*_chain, alias]
    )

  def __init__(self, url: str):
    self.url = url

  @property
  def locator_parameters(self) -> Dict[str, any]:
    return ResourceLocator.strip_locator_parameters(url=self.url)[1]

  @property
  def url_parts(self) -> urllib.parse.ParseResult:
    return ResourceLocator.get_url_parts(url=self.url)
  
  def get_locator_parameter(self, parameter: str) -> Optional[str]:
    locator_parameters = self.locator_parameters
    return locator_parameters[parameter] if parameter in locator_parameters else None

  def get_locator_flag(self, parameter: str) -> bool:
    return ResourceLocator.locator_flag_value(raw_value=self.get_locator_parameter(parameter=parameter))

  def transform(self, resource: Optional[any]) -> Optional[any]:
    if resource is None:
      return resource
    transformed = resource
    if 'encrypt' in self.locator_parameters:
      encrypt_transformer = EncryptTransformer(
        resource=transformed,
        name=self.locator_parameters['encrypt']
      )
      transformed = encrypt_transformer.transform()
    return transformed
  
  def untransform(self, resource: Optional[any]) -> Optional[any]:
    if resource is None:
      return resource
    untransformed = resource
    if 'encrypt' in self.locator_parameters:
      decrypt_untransformer = DecryptUntransformer(resource=untransformed)
      untransformed = decrypt_untransformer.untransform()
    if 'compress' in self.locator_parameters and self.locator_parameters['compress'] == 'zip':
      zip_untransformer = ZipUntransformer(resource=untransformed)
      untransformed = zip_untransformer.untransform()
    if 'encode' in self.locator_parameters:
      decode_untransformer = DecodeUntransformer(
        resource=untransformed,
        encoding=self.locator_parameters['encode']
      )
      untransformed = decode_untransformer.untransform()
    if 'type' in self.locator_parameters and self.locator_parameters['type'] == 'json':
      json_untransformer = JSONUntransformer(resource=untransformed)
      untransformed = json_untransformer.untransform()
    return untransformed
  
  @handle_location_error
  def get(self) -> any:
    raise NotImplementedError()

  @handle_location_error
  def put(self, resource: Optional[any]):
    raise NotImplementedError()

  @handle_location_error
  def delete(self):
    raise NotImplementedError()

  @handle_location_error
  def list(self) -> Optional[List[any]]:
    raise NotImplementedError()

def locator_factory(url: str) -> ResourceLocator:
  dealiased_url = ResourceLocator.dealias_url(url=url)
  parts = urllib.parse.urlparse(dealiased_url)
  if parts.scheme == 'file' or parts.scheme == '':
    return FileLocator(url=dealiased_url)
  elif parts.scheme == 's3':
    return S3Locator(url=dealiased_url)
  elif parts.scheme == 'ssh':
    return SSHLocator(url=dealiased_url)
  elif parts.scheme == 'constant':
    return ConstantLocator(url=dealiased_url)
  else:
    return BaseLocator(url=dealiased_url)

class BaseLocator(ResourceLocator):
  @handle_location_error
  def get(self) -> Dict[str, any]:
    with urllib.request.urlopen(self.url) as response:
      content = response.read()
    return content

def check_safe_file_path(f: Callable[[ResourceLocator, any], any]) -> Callable[[ResourceLocator, ...], any]:
  def wrapper(self, *args, **kwargs):
    path = self.url_parts.path
    if self.safe and not os.path.abspath(path).startswith(f'{os.getcwd()}/'):
      raise ValueError('Cannot target a file path outside the current working directory in safe mode', path)
    return f(self, *args, **kwargs)
  return wrapper

class FileLocator(ResourceLocator):
  @handle_location_error
  @check_safe_file_path
  @untransform_resource
  def get(self) -> any:
    params = self.locator_parameters
    encoding = params['encoding'] if 'encoding' in params else 'binary'
    kwargs = {
      'file': self.url_parts.path,
      'mode': 'rb' if encoding == 'binary' else 'r',
      **({'encoding': encoding} if encoding != 'binary' else {})
    }

    with open(**kwargs) as f:
      content = f.read()
    return content

  @handle_location_error
  @check_safe_file_path
  @transform_resource
  def put(self, resource: Optional[any]):
    path = self.url_parts.path
    params = self.locator_parameters
    if path.endswith('/'):
      dirmode = int(params['dirmode'], 8) if 'dirmode' in params else 0o777
      os.mkdir(path, mode=dirmode)
    else:
      filemode = int(params['filemode'], 8) if 'filemode' in params else None
      encoding = params['encoding'] if 'encoding' in params else 'binary'
      file = os.open(self.url_parts.path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, filemode) if filemode is not None else self.url_parts.path
      kwargs = {
        'file': file,
        'mode': 'wb' if encoding == 'binary' else 'w',
        **({'encoding': encoding} if encoding != 'binary' else {})
      }
      with open(**kwargs) as f:
        f.write(resource)

  @handle_location_error
  @check_safe_file_path
  def delete(self):
    path = self.url_parts.path
    if path.endswith('/'):
      shutil.rmtree(path)
    else:
      os.remove(path)

  @handle_location_error
  @check_safe_file_path
  def list(self):
    path = self.url_parts.path
    if not os.path.exists(path):
      return None
    _, directories, files = next(os.walk(path))
    return sorted([
      *[f'{d}/' for d in directories],
      *files,
    ])

class S3Locator(ResourceLocator):
  s3: Optional[boto3.session.Session.resource] = None

  def __init__(self, url: str):
    super().__init__(url=url)
    region = self.get_locator_parameter('region')
    assert region is not None
    self.s3 = boto3.resource(
      service_name='s3',
      region_name=region,
      aws_access_key_id=self.url_parts.username,
      aws_secret_access_key=self.url_parts.password
    )
  
  @property
  def bucket(self) -> str:
    return self.url_parts.hostname
  
  @property
  def file_key(self) -> str:
    return self.url_parts.path[1:]

  @handle_location_error
  @untransform_resource
  def get(self) -> any:
    json_body = self.s3.meta.client.get_object(
      Bucket=self.bucket,
      Key=self.file_key
    )['Body']
    content = json_body.read()
    return content

  @handle_location_error
  @transform_resource
  def put(self, resource: Optional[any]):
    kwargs = {
      'Bucket': self.bucket,
      'Key': self.file_key,
      **({'Body': resource} if resource else {})
    }
    self.s3.meta.client.put_object(**kwargs)

  @handle_location_error
  def delete(self):
    self.s3.meta.client.delete_object(
      Bucket=self.bucket,
      Key=self.file_key
    )

  @handle_location_error
  def list(self) -> Optional[List[any]]:
    result = self.s3.meta.client.list_objects(
      Bucket=self.bucket,
      Prefix=self.file_key
    )
    file_key_length = len(self.file_key)
    if 'Contents' not in result:
      return None
    return list(filter(lambda s: s, [
      o.get('Key')[file_key_length:]
      for o in result.get('Contents')
    ]))

class SSHLocator(ResourceLocator):
  @handle_location_error
  @untransform_resource
  def get(self) -> any:
    mkdtemp_args = {
      k: v
      for k, v in self.locator_parameters.items() if k in ['dir']
    }
    temp_path = Path(tempfile.mkdtemp(**mkdtemp_args))
    atexit.register(lambda : shutil.rmtree(str(temp_path)) if temp_path.exists() else None)
    recursive = self.url_parts.path[1:].endswith('/')
    remote_path = Path(self.url_parts.path[1:])
    local_path = temp_path / Path(remote_path).name
    run_args = [
      'scp',
      *([] if self.get_locator_flag('verbose') else ['-q']),
      *(['-r'] if recursive else []),
      f'{self.url_parts.netloc}:{remote_path}',
      str(local_path),
    ]
    return_code = subprocess.call(run_args)
    assert return_code == 0
    return os.path.join(str(local_path), '') if recursive else str(local_path)

  @handle_location_error
  @transform_resource
  def put(self, resource: Optional[any]):
    recursive = resource.endswith(os.path.sep)
    local_path = Path(resource)
    remote_path = Path(self.url_parts.path[1:])
    run_args = [
      'scp',
      *([] if self.get_locator_flag('verbose') else ['-q']),
      *(['-r'] if recursive else []),
      str(local_path),
      f'{self.url_parts.netloc}:{remote_path}',
    ]
    return_code = subprocess.call(run_args)
    assert return_code == 0

  @handle_location_error
  def delete(self):
    raise NotImplementedError()

  @handle_location_error
  def list(self) -> Optional[List[any]]:
    remote_path = Path(self.url_parts.path[1:])
    run_args = [
      'ssh',
      self.url_parts.netloc,
      f'cd {shlex.quote(str(remote_path))} && find . -type f && find . -type d | awk \'{{print $0"/"}}\''
    ]
    result = subprocess.run(
      args=run_args,
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE
    )
    assert result.returncode == 0
    output = sorted([
      o.split('./', maxsplit=1)[-1]
      for o in result.stdout.decode().split('\n')
      if o not in ['', '.', './']
    ])
    return output

class ConstantLocator(ResourceLocator):
  @handle_location_error
  @untransform_resource
  def get(self) -> any:
    constant = self.locator_parameters['constant'] if 'constant' in self.locator_parameters else None
    return constant

  @handle_location_error
  def list(self) -> Optional[List[any]]:
    return [self.url_parts.path]

  def untransform(self, resource: Optional[any]) -> Optional[any]:
    unstransformed = resource
    if unstransformed is not None and 'decoding' in self.locator_parameters:
      unstransformed = unstransformed.encode(encoding=self.locator_parameters['decoding'])
    return super().untransform(resource=unstransformed)
