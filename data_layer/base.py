import sqlalchemy as alchemy
import pandas as pd

from sqlalchemy.orm import sessionmaker
from typing import Dict, Tuple, Optional, List, Generic, TypeVar
from datetime import datetime

T = TypeVar(any)
class SQLLayer(Generic[T]):
  class ConnectionOptions:
    user: str
    password: str
    schema: str
    host: str
    port: int
    database: str
    connector_options: Dict[str, any]

    def __init__(self, options: Dict[str, any]):
      self.user = options['user']
      self.password = options['password']
      self.schema = options['schema']
      self.host = options['host']
      self.port = options['port']
      self.database = options['database']
      self.connector_options = options['connector_options']

    @property
    def dictionary_representation(self):
      return {
        'user': self.user,
        'password': self.password,
        'host': self.host,
        'port': self.port,
        'database': self.database,
        'connector_options': self.connector_options,
      }

  # SQLLayer properties
  connection: T = None
  alchemy_engine: alchemy.engine.Engine
  alchemy_session_maker: sessionmaker
  connection_options: ConnectionOptions
  echo: bool

  @property
  def engine_url(self) -> str:
    return f'{self.connection_options.schema}://{self.connection_options.user}:{self.connection_options.password}@{self.connection_options.host}:{self.connection_options.port}/{self.connection_options.database}'

  @property
  def engine_connector_arguments(self) -> Dict[str, any]:
    return {}

  def __init__(self, connection_options: ConnectionOptions=None, echo: bool=False, alchemy_echo: bool=False):
    self.echo = echo
    if connection_options is not None:
      self.connection_options = connection_options
    
    self.alchemy_engine = alchemy.create_engine(
      self.engine_url, 
      connect_args=self.engine_connector_arguments,
      echo=alchemy_echo
    )
    self.alchemy_session_maker = alchemy.orm.sessionmaker(bind=self.alchemy_engine)
  
  @classmethod
  def configure_connection(cls, options: Optional[Dict[str, any]]):
    if options is None:
      if hasattr(cls, 'connection_options'):
        del cls.connection_options
    else:
      cls.connection_options = cls.ConnectionOptions(options)
  
  def echo_text(self, cursor: any) -> str:
    return ''

  def alchemy_session(self) -> alchemy.orm.session.Session:
    return self.alchemy_session_maker()

  def connect(self):
    raise NotImplementedError()

  def disconnect(self):
    self.connection.close()
    self.connection = None

  def commit(self):
    self.connection.commit()

  def rollback(self):
    self.connection.rollback()

  def query(self, query: str, substitution_parameters=()) -> any:
    """Returns a cursor to access the SQL data from the query"""
    cursor = self.connection.cursor()      
    cursor.execute(query, substitution_parameters)

    if self.echo:
      print(self.echo_text(cursor=cursor))
    return cursor
  
  def table_exists(self, table_name: str, schema_name: Optional[str]=None) -> bool:
    raise NotImplementedError()

  def schema_exists(self, schema_name: str) -> bool:
    raise NotImplementedError()

  def insert_data_frame(self, data_frame: pd.DataFrame, table_name: str, schema_name: Optional[str]=None, column_type_transform_dictionary: Optional[Dict[str, any]] = None, chunksize: int=1000):
    """Writes a pandas data frame to the SQL database"""
    data_frame.to_sql(
      name=table_name,
      schema=schema_name,
      con=self.alchemy_engine,
      if_exists='append',
      index=False,
      chunksize=chunksize,
      dtype=column_type_transform_dictionary
    )

  def fetch_one_record(self, cursor: any) -> Dict[str, any]:
    raise NotImplementedError()

  def fetch_all_records(self, cursor: any) -> List[Dict[str, any]]:
    raise NotImplementedError()

  def fetch_data_frame(self, cursor: any) -> pd.DataFrame:
    return pd.DataFrame(self.fetch_all_records(cursor=cursor))