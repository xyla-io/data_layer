from __future__ import annotations
import os

from .error import EncryptionMetadataError, EncryptionRegistryError
from typing import Optional, Dict, Tuple, List
from functools import reduce
from hashlib import sha1
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding as symmetric_padding
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from contextlib import contextmanager

class Cryptor:
  encryptor_registries: List[Dict[str, Encryptor]]=[]
  decryptor_registries: List[Dict[str, Decryptor]]=[]

  @classmethod
  def push_registries(cls):
    cls.encryptor_registries.append({**Encryptor.registry})
    Encryptor.registry.clear()
    cls.decryptor_registries.append({**Decryptor.registry})
    Decryptor.registry.clear()

  @classmethod
  def pop_registries(cls):
    Encryptor.registry.clear()
    Encryptor.registry.update(cls.encryptor_registries.pop())
    Decryptor.registry.clear()
    Decryptor.registry.update(cls.decryptor_registries.pop())

  @staticmethod
  @contextmanager
  def local_registries():
    Cryptor.push_registries()
    try:
      yield
    finally:
      Cryptor.pop_registries()

class Encryptor:
  """A class for encrypting data."""

  class Metadata:
    """Encryption metadata for a particular file"""
    key: bytes
    initialization_vector: bytes
    name: str

    def __init__(self, key: bytes, initialization_vector: bytes, name: str):
      self.key = key
      self.initialization_vector = initialization_vector
      self.name = name

    @classmethod
    def from_dictionary(cls, dictionary: Dict[str, str]):
      return cls(
        key=bytes.fromhex(dictionary['key']),
        initialization_vector=bytes.fromhex(dictionary['initialization_vector']),
        name=dictionary['name']
      )

    def to_dictionary(self) -> Dict[str, str]:
      return {
        'key': self.key.hex(),
        'initialization_vector': self.initialization_vector.hex(),
        'name': self.name,
      }

  public_key: any
  backend: any
  name: str
  registry: Dict[str, 'Encryptor']={}

  @classmethod
  def hash(cls, bytes: bytes) -> bytes:
    return sha1(bytes).digest()

  @classmethod
  def get_key_padder(cls):
    padder = padding.OAEP(
      mgf=padding.MGF1(algorithm=hashes.SHA256()),
      algorithm=hashes.SHA256(),
      label=None
    )
    return padder

  @classmethod
  def key_cipher(cls, key: bytes, initialization_vector: bytes, backend: any):
    cipher = Cipher(
      algorithms.AES(key),
      modes.CBC(initialization_vector),
      backend=backend
    )
    return cipher

  @classmethod
  def get_padding(cls):
    padding = symmetric_padding.PKCS7(128)
    return padding

  @classmethod
  def pad(cls, data: bytes) -> bytes:
    padder = cls.get_padding().padder()
    padded_bytes = padder.update(data) + padder.finalize()
    return padded_bytes

  @classmethod
  def unpad(cls, data: bytes) -> bytes:
    unpadder = cls.get_padding().unpadder()
    unpadded_bytes = unpadder.update(data) + unpadder.finalize()
    return unpadded_bytes

  @classmethod
  def encipher(cls, data: bytes, key: bytes, initialization_vector: bytes, backend: any):
    padded_bytes = cls.pad(data=data)
    cipher = cls.key_cipher(
      key=key,
      initialization_vector=initialization_vector,
      backend=backend
    )
    encryptor = cipher.encryptor()
    enciphered = encryptor.update(padded_bytes) + encryptor.finalize()
    return enciphered

  @classmethod
  def decipher(cls, data: bytes, key: bytes, initialization_vector: bytes, backend: any):
    cipher = cls.key_cipher(
      key=key,
      initialization_vector=initialization_vector,
      backend=backend
    )
    decryptor = cipher.decryptor()
    padded_bytes = decryptor.update(data) + decryptor.finalize()
    unpadded_bytes = cls.unpad(data=padded_bytes)
    return unpadded_bytes

  @classmethod
  def register_encryptor(cls, encryptor: Encryptor):
    cls.registry[encryptor.name] = encryptor

  @classmethod
  def get_registered_encryptor(cls, name: str) -> Encryptor:
    if name not in cls.registry:
      raise EncryptionRegistryError(f"No encryptor registered for name '{name}'")
    return cls.registry[name]

  @classmethod
  def encrypt_with_registry(cls, data: bytes, name: str, key: Optional[bytes]=None, initialization_vector: Optional[bytes]=None, append_metadata: bool=False) -> Optional[Tuple[bytes, Encryptor.metadata]]:
    encryptor = cls.get_registered_encryptor(name=name)
    if encryptor is None:
      return None
    encrypted, metadata = encryptor.encrypt(
      data=data,
      key=key,
      initialization_vector=initialization_vector
    )
    if append_metadata:
      encrypted = cls.append_metadata(
        data=encrypted,
        metadata=metadata
      )
    return (encrypted, metadata)

  @classmethod
  def append_metadata(cls, data: bytes, metadata: Encryptor.Metadata) -> bytes:
    parts = [
      metadata.key,
      metadata.initialization_vector,
      metadata.name.encode(encoding='utf-8'),
      data,
    ]
    joined_data = reduce(lambda j, d: j + len(d).to_bytes(length=8, byteorder='big') + d, parts, b'')
    return joined_data

  @classmethod
  def public_key_bytes(cls, public_key: any) -> bytes:
    return public_key.public_bytes(
      encoding=serialization.Encoding.PEM,
      format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

  def __init__(self, public_key: bytes, name: Optional[str]=None):
    self.backend = default_backend()
    self.public_key = serialization.load_pem_public_key(
      public_key,
      backend=self.backend
    )
    self.name = name if name is not None else Encryptor.hash(self.get_public_bytes()).hex()

  def get_public_bytes(self) -> bytes:
    return Encryptor.public_key_bytes(
      public_key=self.public_key
    )

  def generate_initialization_vector(self) -> bytes:
    iv = os.urandom(16)
    return iv

  def generate_key(self) -> bytes:
    key = os.urandom(32)
    return key

  def generate_metadata(self) -> Encryptor.Metadata:
    return Encryptor.Metadata(
      key=self.generate_key(),
      initialization_vector=self.generate_initialization_vector(),
      name=self.name
    )

  def encrypted_key(self, key_bytes: bytes) -> bytes:
    encrypted_key_bytes = self.public_key.encrypt(
      key_bytes,
      padding=Encryptor.get_key_padder()
    )
    return encrypted_key_bytes

  def encrypt(self, data: bytes, key: Optional[bytes]=None, initialization_vector: Optional[bytes]=None) -> Tuple[bytes, Encryptor.Metadata]:
    if key is None:
      key = self.generate_key()
    if initialization_vector is None:
      initialization_vector = self.generate_initialization_vector()

    enciphered = Encryptor.encipher(
      data=data,
      key=key,
      initialization_vector=initialization_vector,
      backend=self.backend
    )
    metadata = Encryptor.Metadata(
      key=self.encrypted_key(key_bytes=key),
      initialization_vector=initialization_vector,
      name=self.name
    )
    return (enciphered, metadata)

class Decryptor:
  """A class for decrypting data."""
  private_key: any
  backend: any
  name: str
  registry: Dict[str, 'Decryptor']={}

  @classmethod
  def register_decryptor(cls, decryptor: Decryptor):
    cls.registry[decryptor.name] = decryptor
    Encryptor.register_encryptor(decryptor.get_encryptor())

  @classmethod
  def get_registered_decryptor(cls, name: str) -> Decryptor:
    if name not in cls.registry:
      raise EncryptionRegistryError(f"No decryptor registered for name '{name}'")
    return cls.registry[name]

  @classmethod
  def decrypt_with_registry(cls, data: bytes, metadata: Optional[Encryptor.Metadata]=None) -> Optional[bytes]:
    if metadata is None:
      encrypted, metadata = cls.strip_metadata(data=data)
    else:
      encrypted = data
    decryptor = cls.get_registered_decryptor(name=metadata.name)
    if decryptor is None:
      return None
    return decryptor.decrypt(
      data=encrypted,
      metadata=metadata
    )

  @classmethod
  def generate_private_key(cls, password: Optional[bytes], public_exponent: int=65537, key_size: int=4096) -> bytes:
    private_key = rsa.generate_private_key(
      public_exponent=public_exponent,
      key_size=key_size,
      backend=default_backend()
    )
    return cls.private_key_bytes(
      private_key=private_key,
      password=password
    )

  @classmethod
  def private_key_bytes(cls, private_key: any, password: Optional[bytes]):
    kwargs = {
      'encoding': serialization.Encoding.PEM,
      'format': serialization.PrivateFormat.PKCS8,
      'encryption_algorithm': serialization.BestAvailableEncryption(password) if password else serialization.NoEncryption(),
    }
    return private_key.private_bytes(**kwargs)

  @classmethod
  def strip_metadata(cls, data: bytes) -> Tuple[bytes, Encryptor.Metadata]:
    byte_index = 0
    parts = []
    for remaining_parts in reversed(range(4)):
      try:
        assert len(data) >= byte_index + 8, 'Not enough part length bytes when stripping metadata'
        part_length = int.from_bytes(data[byte_index:byte_index + 8], byteorder='big')
        byte_index += 8
        assert len(data) >= byte_index + part_length, 'Not enough part bytes when stripping metadata'
        parts.append(data[byte_index: byte_index + part_length])
        byte_index += part_length
        if not remaining_parts:
          assert len(data) == byte_index, 'Extra bytes remaining after stripping metadata'
      except (KeyboardInterrupt, SystemExit):
        raise
      except Exception as e:
        raise EncryptionMetadataError(error=e)
    
    try:
      metadata = Encryptor.Metadata(
        key=parts[0],
        initialization_vector=parts[1],
        name=parts[2].decode(encoding='utf-8')
      )
    except (KeyboardInterrupt, SystemExit):
      raise
    except Exception as e:
      raise EncryptionMetadataError(error=e)    
    return (parts[3], metadata)

  def __init__(self, private_key: bytes, password: Optional[str]=None, name: Optional[str]=None):
    self.backend = default_backend()
    self.private_key = serialization.load_pem_private_key(
      private_key,
      password=password,
      backend=self.backend
    )
    self.name = name if name is not None else Encryptor.hash(Encryptor.public_key_bytes(self.private_key.public_key())).hex()

  def get_private_bytes(self, password: Optional[bytes]) -> bytes:
    return Decryptor.private_key_bytes(
      private_key=self.private_key,
      password=password
    )

  def get_encryptor(self) -> Encryptor:
    return Encryptor(
      public_key=Encryptor.public_key_bytes(
        public_key=self.private_key.public_key()
      ),
      name=self.name
    )

  def decrypted_key(self, encrypted_key_bytes: bytes) -> bytes:
    key_bytes = self.private_key.decrypt(
      encrypted_key_bytes,
      padding=Encryptor.get_key_padder()
    )
    return key_bytes

  def decrypt(self, data: bytes, metadata: Encryptor.Metadata) -> bytes:
    decrypted_key = self.decrypted_key(encrypted_key_bytes=metadata.key)
    deciphered = Encryptor.decipher(
      data=data,
      key=decrypted_key,
      initialization_vector=metadata.initialization_vector,
      backend=self.backend
    )
    return deciphered




