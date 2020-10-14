from typing import Optional, List

class SQLError(Exception):
  pass

class SQLExecutionError(SQLError):
  pass

class SQLStatementExecutionError(SQLExecutionError):
  statement: str

  def __init__(self, statement: str, errors: List[Exception]):
    self.statement = statement
    linebreak = '\n'
    error_text = f' ({len(errors)}) errors:\n{linebreak.join(repr(e) for e in errors)}' if len(errors) > 1 else f' : {repr(errors[0])}' if errors else ''
    super().__init__(f'Execution Error for statement {self.statement}{error_text}')

class LocationError(Exception):
  url: str

  def __init__(self, url: str, error: Optional[Exception]=None):
    self.url = url
    super().__init__(f'Location Error for URL {url}{f": {repr(error)}" if error else ""}')

class LocationRegistryError(LocationError):
  def __init__(self, alias: str, error: Optional[Exception]=None):
    super().__init__(url=f'alias://{alias}', error=KeyError(alias) if error is None else error)

class LocationRegistryCircularDependencyError(LocationRegistryError):
  def __init__(self, chain: List[str]):
    super().__init__(alias=chain[-1], error=ValueError('Circular alias chain', chain))

class EncryptionError(Exception):
  def __init__(self, message: Optional[str]=None, error: Optional[Exception]=None):
    super().__init__(f'Encription Error{f" ({message})" if message else ""}{f": {repr(error)}" if error else ""}')

class EncryptionMetadataError(EncryptionError):
  pass

class EncryptionRegistryError(EncryptionError):
  pass