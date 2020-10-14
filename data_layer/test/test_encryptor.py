import pytest

from ..encryptor import Encryptor, Decryptor
from secrets import token_bytes, randbelow

@pytest.fixture
def private_key_bytes():
  private_key_bytes = Decryptor.generate_private_key(
    password=None
  )
  yield private_key_bytes


@pytest.fixture
def decryptor(private_key_bytes):
  decryptor = Decryptor(
    private_key=private_key_bytes,
    password=None
  )
  yield decryptor

@pytest.fixture
def encryptor(decryptor):
  encryptor = decryptor.get_encryptor()
  yield encryptor

@pytest.fixture
def secret_bytes():
  secret_bytes = token_bytes(512 + randbelow(512))
  yield secret_bytes

@pytest.fixture
def long_secret_bytes(secret_bytes):
  long_secret_bytes = b''.join(int.to_bytes(i, length=4, byteorder='big') + secret_bytes for i in range(0, 512 + randbelow(512)))
  yield long_secret_bytes

def test_decryptor_generate_private_key(private_key_bytes):
  assert private_key_bytes.find(b'-----BEGIN PRIVATE KEY-----') != -1

def test_decryptor_private_key_bytes(private_key_bytes, decryptor):
  assert decryptor.get_private_bytes(password=None) == private_key_bytes

def test_encryptor_public_key_byptes(encryptor):
  assert encryptor.get_public_bytes().find(b'-----BEGIN PUBLIC KEY-----') != -1

def test_name_hash(encryptor):
  assert encryptor.name == Encryptor.hash(encryptor.get_public_bytes()).hex()

def test_encryption(encryptor, secret_bytes):
  encrypted, _ = encryptor.encrypt(
    data=secret_bytes
  )
  assert len(encrypted)
  assert encrypted != secret_bytes

def test_decryption(encryptor, secret_bytes, decryptor):
  encrypted, metadata = encryptor.encrypt(
    data=secret_bytes
  )
  decrypted = decryptor.decrypt(
    data=encrypted,
    metadata=metadata
  )
  assert decrypted == secret_bytes

def test_appended_metadata(encryptor, long_secret_bytes, decryptor):
  encrypted = Encryptor.append_metadata(*encryptor.encrypt(
    data=long_secret_bytes
  ))
  decrypted = decryptor.decrypt(*Decryptor.strip_metadata(
    data=encrypted,
  ))
  assert decrypted == long_secret_bytes

def test_key(encryptor):
  key = encryptor.generate_key()
  assert len(key)
  assert key != encryptor.generate_key()

def test_initialization_vector(encryptor):
  initialization_vector = encryptor.generate_initialization_vector()
  assert len(initialization_vector)
  assert initialization_vector != encryptor.generate_initialization_vector()

def test_cipher(encryptor, secret_bytes):
  key = encryptor.generate_key()
  initialization_vector = encryptor.generate_initialization_vector()
  enciphered = Encryptor.encipher(
    data=secret_bytes,
    key=key,
    initialization_vector=initialization_vector,
    backend=encryptor.backend
  )
  assert enciphered != secret_bytes
  deciphered = Encryptor.decipher(
    data=enciphered,
    key=key,
    initialization_vector=initialization_vector,
    backend=encryptor.backend
  )
  assert deciphered == secret_bytes
  deciphered_different_key = None
  try:
    deciphered_different_key = Encryptor.decipher(
      data=enciphered,
      key=encryptor.generate_key(),
      initialization_vector=initialization_vector,
      backend=encryptor.backend
    )
  except (KeyboardInterrupt, SystemExit):
    raise
  except Exception:
    pass
  assert deciphered_different_key is None or deciphered_different_key != secret_bytes
  deciphered_different_initialization_vector = None
  try:
    deciphered_different_initialization_vector = Encryptor.decipher(
      data=enciphered,
      key=key,
      initialization_vector=encryptor.generate_initialization_vector(),
      backend=encryptor.backend
    )
  except (KeyboardInterrupt, SystemExit):
    raise
  except Exception:
    pass
  assert deciphered_different_initialization_vector is None or deciphered_different_initialization_vector != secret_bytes

def test_registry(encryptor, long_secret_bytes, decryptor):
  Decryptor.register_decryptor(decryptor=decryptor)
  encrypted, _ = Encryptor.encrypt_with_registry(
    data=long_secret_bytes,
    name=encryptor.name,
    append_metadata=True
  )
  decrypted = Decryptor.decrypt_with_registry(
    data=encrypted
  )
  assert decrypted == long_secret_bytes
