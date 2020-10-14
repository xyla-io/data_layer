import json

from zipfile import ZipFile
from io import BytesIO
from .encryptor import Encryptor, Decryptor

class Transformer:
  original_resource: any

  def __init__(self, resource: any):
    self.original_resource = resource
  
  def transform(self) -> any:
    raise NotImplementedError()

class EncryptTransformer(Transformer):
  name: str

  def __init__(self, resource: any, name: str):
    self.name = name
    super().__init__(resource=resource)

  def transform(self) -> bytes:
    encrypted, _ = Encryptor.encrypt_with_registry(
      data=self.original_resource,
      name=self.name,
      append_metadata=True
    )
    return encrypted

class Untransformer:
  original_resource: any

  def __init__(self, resource: any):
    self.original_resource = resource
  
  def untransform(self) -> any:
    raise NotImplementedError()

class ZipUntransformer(Untransformer):
  def untransform(self) -> ZipFile:
    with BytesIO(self.original_resource) as data:
      with ZipFile(data) as zip_file:
        return {
          file: zip_file.read(file)
          for file in zip_file.namelist()
        }

class DecryptUntransformer(Untransformer):
  def untransform(self) -> bytes:
    return Decryptor.decrypt_with_registry(data=self.original_resource)

class DecodeUntransformer(Untransformer):
  encoding: str

  def __init__(self, resource: bytes, encoding: str):
    self.encoding = encoding
    super().__init__(resource=resource)

  def untransform(self) -> str:
    return self.original_resource.decode(encoding=self.encoding)

class JSONUntransformer(Untransformer):
  def untransform(self) -> any:
    return json.loads(self.original_resource)
