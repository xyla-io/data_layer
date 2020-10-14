import re
import json
import bson
import urllib
import datetime
import pandas as pd

from ..base import SQLLayer
from ..query import SQLQuery
from ..error import SQLError, SQLStatementExecutionError
from typing import Optional, Tuple, List, Dict, Callable
from pymongo import MongoClient
from sshtunnel import SSHTunnelForwarder
from moda.evaluate import evaluate

class MongoQuery(SQLQuery):
  @staticmethod
  def escaped_query_text(query_text: str) -> str:
    return query_text.replace('${', '$${')

  @staticmethod
  def formatted_query_text(query_text: str, format_parameters: Dict[str, any]) -> str:
    format_string = re.sub(r'^\s*//[^\n]*$', r'', query_text, 0, re.MULTILINE)
    format_string = format_string.replace('{', '{{').replace('}', '}}')
    format_string = re.sub(r'"\${{ *f *>(([^}]|}}}})*)}}"', r'\\"${{f>\1}}\\"', format_string)
    format_string = re.sub(r'\\"\${{ *f *>(([^}]|}}}})*)}}\\"', lambda m: f"{{{m[1].replace('{{{{', '{{').replace('}}}}', '}}')}}}", format_string)
    formatted_text = format_string.format(**format_parameters)
    return formatted_text

  @property
  def substituted_query(self) -> str:
    return self.query

class MongoCursor:
  statement_index: int
  statements: List[any]
  substitution_parameters: Tuple[any]
  substituted_statements: List[Dict[str, any]]
  results: List[Dict[str, any]]
  connection: MongoClient
  schema: str

  def __init__(self, connection: MongoClient, schema: str, statements: List[any], substitution_parameters: Tuple[any]):
    self.statements = statements
    self.substitution_parameters = substitution_parameters
    self.connection = connection
    self.schema = schema
    self.statement_index = 0
    self.substituted_statements = []
    self.results = []

  def package_fetches(self, fetches: List[Dict[str, any]], index: int=0):
    return {
      'index': 0,
      'batch_start_index': 0,
      'fetches': fetches,
    }

  def package_batch(self, batch: List[Dict[str, any]], id: int=0, collection_name: Optional[str]=None):
    return {
      'cursor': {
        'firstBatch': batch,
        'id': id,
        **({'ns': f'{self.schema}.{collection_name}'} if collection_name is not None else {}),
      },
      'ok': 1.0,
    }

  def substituted_statement(self, statement: Dict[str, any]) -> Dict[str, any]:
    def package_results(batch: List[Dict[str, any]], id: int=0, collection_name: Optional[str]=None):
      fetch = self.package_results(
        batch=batch,
        id=id,
        collection_name=collection_name
      )
      return self.package_fetches(fetches=[fetch])

    def transformer(value: any):
      if not isinstance(value, str):
        return value
      match = re.fullmatch(r'\${ *([^}]) *>(([^}]|}})*)}', value)
      if not match:
        return value.replace('$${', '${')
      
      if match[1] == 's':
        return self.substitution_parameters[int(match[2]) - 1]
      elif match[1] == 'p':
        code = match[2].replace('{{', '{').replace('}}', '}')
        context = {
          'v': None,
          'cursor': self,
          'db': self.connection[self.schema],
          'connection': self.connection,
          'bson': bson,
          'datetime': datetime,
          'package_results': package_results,
        }
        evaluate(
          code=code,
          context=context
        )
        return context['v']
      else:
        raise ValueError('Unsupported query substitution type', match[1])
    
    substituted_statement = MongoLayer.transform_leaves(
      structure=statement,
      transformer=transformer
    )
    return substituted_statement

  def execute_statement(self, statement: Dict[str, any]) -> Dict[str, any]:
    result = self.connection[self.schema].command(statement)
    for container in [
      result,
      *[result['cursor'] if 'cursor' in result else []],
    ]:
      for key in container:
        if key.endswith('Errors') and container[key]:
          raise SQLStatementExecutionError(
            statement=json.dumps(statement),
            errors=[SQLError(json.dumps(e)) for e in container[key]]
          )
    return result

  def execute_next_statement(self) -> bool:
    if self.statement_index >= len(self.statements):
      return False
    statement = self.statements[self.statement_index]
    substituted_statement = self.substituted_statement(statement=statement)
    if isinstance(substituted_statement, tuple):
      assert len(substituted_statement) == 2
      self.substituted_statements.append(substituted_statement[0])
      self.results.append(substituted_statement[1])      
    elif isinstance(substituted_statement, dict):
      self.substituted_statements.append(substituted_statement)
      result = self.execute_statement(statement=substituted_statement)
      self.results.append(self.package_fetches(fetches=[result]))
    elif isinstance(substituted_statement, list):
      self.substituted_statements.append({})
      self.results.append(self.package_fetches(fetches=self.package_batch(batch=substituted_statement)))
    else:
      self.substituted_statements.append({})
      self.results.append(self.package_fetches(fetches=self.package_batch(batch=[substituted_statement])))
    self.statement_index += 1
    return True

  def fetchone(self, result_index: Optional[int]=None, item_index: Optional[int]=None) -> Dict[str, any]:
    if result_index is None:
      result_index = self.statement_index - 1
    if not self.results:
      raise StopIteration(0)
    return self.fetchone_from_result(
      result=self.results[result_index],
      item_index=item_index
    )

  def fetchone_from_result(self, result: Dict[str, any], item_index: Optional[int]=None) -> Dict[str, any]:
    target_index = item_index if item_index is not None else result['index']
    fetches = result['fetches']
    if not fetches:
      raise StopIteration(0)
    if target_index < result['batch_start_index']:
      raise IndexError('item_index out of range')

    fetch_index = 0
    batch_start_index = 0
    while True:
      last_result = fetches[fetch_index]

      if 'cursor' not in last_result:
        raise StopIteration(batch_start_index)

      last_cursor = last_result['cursor']
      batch = last_cursor['firstBatch'] if 'firstBatch' in last_cursor else last_cursor['nextBatch'] if 'nextBatch' in last_cursor else None
      if batch is None:
        raise StopIteration(batch_start_index)

      if len(batch) > target_index - batch_start_index:
        if item_index is None:
          result['batch_start_index'] = batch_start_index
          result['index'] = target_index + 1
        return batch[target_index - batch_start_index]

      batch_start_index += len(batch)
      if fetch_index < len(fetches) - 1:
        fetch_index += 1
        continue

      if 'id' not in last_cursor or not last_cursor['id'] or 'ns' not in last_cursor or '.' not in last_cursor['ns']:
        raise StopIteration(batch_start_index)
      
      next_fetch = self.connection[self.schema].command({
        'getMore': last_cursor['id'],
        'collection': last_cursor['ns'].split('.', 1)[1],
      })
      if item_index is None:
        fetches[-1] = next_fetch
      else:
        fetches.append(next_fetch)
        fetch_index += 1

  def fetchall(self, result_index: Optional[int]=None, item_index: Optional[int]=None) -> List[Dict[str, any]]:
    if result_index is None:
      result_index = self.statement_index - 1
    if not self.results:
      raise StopIteration(0)
    return self.fetchall_from_result(
      result=self.results[result_index],
      item_index=item_index
    )

  def fetchall_from_result(self, result: Dict[str, any], item_index: Optional[int]=None) -> List[Dict[str, any]]:
    target_index = item_index if item_index is not None else result['index']
    all_items = []
    fetches = result['fetches']
    if target_index < result['batch_start_index']:
      raise IndexError('item_index out of range')

    fetch_index = 0
    batch_start_index = 0

    def finish():
      if item_index is None:
        result['batch_start_index'] = batch_start_index
        result['index'] = len(all_items) + target_index
      return all_items

    if not fetches:
      return finish()

    while True:
      last_result = fetches[fetch_index]
      if 'cursor' not in last_result:
        return finish()

      last_cursor = last_result['cursor']
      batch = last_cursor['firstBatch'] if 'firstBatch' in last_cursor else last_cursor['nextBatch'] if 'nextBatch' in last_cursor else None
      if batch is None:
        return finish

      if len(batch) > target_index - batch_start_index:
        all_items.extend(batch[max(target_index - batch_start_index, 0):])

      batch_start_index += len(batch)
      if fetch_index < len(fetches) - 1:
        fetch_index += 1
        continue

      if 'id' not in last_cursor or not last_cursor['id'] or 'ns' not in last_cursor or '.' not in last_cursor['ns']:
        return finish()

      next_fetch = self.connection[self.schema].command({
        'getMore': last_cursor['id'],
        'collection': last_cursor['ns'].split('.', 1)[1],
      })
      if item_index is None:
        fetches[-1] = next_fetch
      else:
        fetches.append(next_fetch)
        fetch_index += 1

class MongoLayer(SQLLayer[MongoClient]):
  schema: Optional[str]=None
  ssh_tunnel: Optional[any]=None

  @classmethod
  def transform_leaves(cls, structure: any, transformer: Callable[[any], any]) -> any:
    if isinstance(structure, dict):
      return { cls.transform_leaves(k, transformer): cls.transform_leaves(v, transformer) for k, v in structure.items() }
    elif isinstance(structure, list) or isinstance(structure, tuple) or isinstance(structure, set):
      return [ cls.transform_leaves(v, transformer) for v in structure ]
    else:
      return transformer(structure)

  def __init__(self, connection_options: SQLLayer.ConnectionOptions=None, echo: bool=False, alchemy_echo: bool=False):
    self.echo = echo
    if connection_options is not None:
      self.connection_options = connection_options
    self.schema = self.connection_options.database

  def echo_text(self, cursor: any) -> str:
    return cursor.query

  @property
  def engine_url(self) -> str:
    raise NotImplementedError()

  def connect(self):
    if 'ssh_options' in self.connection_options.connector_options:
      ssh_options = {
        'local_port': 27018,
        **self.connection_options.connector_options['ssh_options']
      }
      self.ssh_tunnel = SSHTunnelForwarder(
        self.connection_options.host,
        ssh_username=ssh_options['user'],
        remote_bind_address=('127.0.0.1', self.connection_options.port),
        local_bind_address=('127.0.0.1', ssh_options['local_port'])
      )
      self.ssh_tunnel.start()
      url = f'mongodb://127.0.0.1:{ssh_options["local_port"]}/{self.connection_options.database}'
    else:
      url = f'mongodb://{self.connection_options.host}:{self.connection_options.port}/{self.connection_options.database}'
    self.connection = MongoClient(url)

  def disconnect(self):
    super().disconnect()
    if self.ssh_tunnel is not None:
      self.ssh_tunnel.stop()

  def get_database(self, schema_name: Optional[str]=None) -> any:
    assert self.schema is None or schema_name is None
    schema = self.schema if schema_name is None else schema_name
    return self.connection[schema]

  def commit(self):
    pass

  def rollback(self):
    raise NotImplementedError()

  def schema_exists(self, schema_name: str) -> bool:
    assert self.schema is None
    return schema_name in self.connection.list_database_names()

  def table_exists(self, table_name: str, schema_name: Optional[str]=None) -> bool:
    db = self.get_database(schema_name=schema_name)
    return table_name in db.list_collection_names()

  def query_cursor(self, query: str, substitution_parameters: Tuple[any]=()):
    return MongoCursor(
      connection=self.connection,
      schema=self.schema,
      statements=json.loads(query),
      substitution_parameters=substitution_parameters
    )

  def query(self, query: str, substitution_parameters=()) -> any:
    cursor = self.query_cursor(
      query=query,
      substitution_parameters=substitution_parameters
    )
    while cursor.execute_next_statement():
      pass
    return cursor

  def insert_data_frame(self, data_frame: pd.DataFrame, table_name: str, schema_name: Optional[str]=None, column_type_transform_dictionary: Optional[Dict[str, any]] = None, chunksize: int=1000):
    raise NotImplementedError()

  def fetch_one_record(self, cursor: any) -> Dict[str, any]:
    return self.sanitize_result(cursor.fetchone())

  def fetch_all_records(self, cursor: any) -> List[Dict[str, any]]:
    return self.sanitize_result(cursor.fetchall())

  def sanitize_result(self, result: any) -> any:
    return MongoLayer.transform_leaves(
      structure=result,
      transformer=lambda l: str(l) if isinstance(l, bson.ObjectId) else l
    )