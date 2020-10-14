import psycopg2
import pandas as pd

from ..base import SQLLayer
from typing import Optional, Callable, Dict, List
from psycopg2 import OperationalError as Psycopg2OperationalError
from time import time

def check_connection(f: Callable[..., any]) -> Callable[..., any]:
  def wrapper(*args, **kwargs):
    layer = args[0]
    if layer.connection_check_interval is not None and layer.connection_checked_time is not None:
      idle_time = time() - layer.connection_checked_time
      if idle_time >= layer.connection_check_interval:
        print(f'Checking database connection after {idle_time:.2f} seconds idle')
        was_connected = layer.connection is not None
        if not was_connected:
          layer.connect()
        try:
          layer.connection_checked_time = None
          layer.query('select null;').close()
          if not was_connected:
            layer.disconnect()
        except Psycopg2OperationalError:
          print('Database connection broken, recovering.')
          layer.connection = None
          if was_connected:
            layer.connect()
    result = f(*args, **kwargs)
    layer.connection_checked_time = time()
    return result
  return wrapper

class PostgreSQLLayer(SQLLayer[any]):
  connection_check_interval: Optional[float]
  connection_checked_time: Optional[float]=None  

  def __init__(self, connection_options: SQLLayer.ConnectionOptions=None, echo: bool=False, alchemy_echo: bool=False, connection_check_interval: Optional[float]=None):
    super().__init__(connection_options=connection_options, echo=echo, alchemy_echo=alchemy_echo)
    if connection_check_interval is None and 'connection_check_interval' in self.connection_options.connector_options:
      connection_check_interval = self.connection_options.connector_options['connection_check_interval']
    if connection_check_interval is not None and connection_check_interval < 0:
      connection_check_interval = None
    self.connection_check_interval = connection_check_interval

  def echo_text(self, cursor: any) -> str:
    return cursor.query

  @property
  def engine_url(self) -> str:
    return f'{super().engine_url}?sslmode={self.connection_options.connector_options["sslmode"]}'

  def connect(self):
    options = self.connection_options.dictionary_representation
    del options['connector_options']
    self.connection = psycopg2.connect(**options, sslmode=self.connection_options.connector_options['sslmode'])

  def schema_exists(self, schema_name: str) -> bool:
    query = """
SELECT EXISTS (
   SELECT 1
   FROM information_schema.schemata 
   WHERE schema_name = %s
   );
    """
    cursor = self.query(query=query, substitution_parameters=(schema_name,))
    result = cursor.fetchone()[0]
    self.commit()
    return result

  def table_exists(self, table_name: str, schema_name: Optional[str]=None) -> bool:
    query = """
SELECT EXISTS (
   SELECT 1
   FROM information_schema.tables 
   WHERE table_schema = %s
   AND table_name = %s
   );
    """
    cursor = self.query(query=query, substitution_parameters=(schema_name, table_name))
    result = cursor.fetchone()[0]
    self.commit()
    return result

  @check_connection
  def query(self, query: str, substitution_parameters=()) -> any:
    return super().query(query=query, substitution_parameters=substitution_parameters)

  @check_connection
  def insert_data_frame(self, data_frame: pd.DataFrame, table_name: str, schema_name: Optional[str]=None, column_type_transform_dictionary: Optional[Dict[str, any]] = None, chunksize: int=1000):
    super().insert_data_frame(data_frame=data_frame, table_name=table_name, schema_name=schema_name, column_type_transform_dictionary=column_type_transform_dictionary, chunksize=chunksize)

  def fetch_one_record(self, cursor: any) -> Dict[str, any]:
    result = cursor.fetchone()
    column_names = [c.name for c in cursor.description]
    record = {
      c: result[i]
      for i, c in enumerate(column_names)
    }
    return record

  def fetch_all_records(self, cursor: any) -> List[Dict[str, any]]:
    results = cursor.fetchall()
    column_names = [c.name for c in cursor.description]
    records = [
      {
        c: r[i]
        for i, c in enumerate(column_names)
      }
      for r in results
    ]
    return records