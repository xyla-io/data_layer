import pandas as pd
import sqlalchemy as alchemy

from . import pandas_redshift as pr
from .redshift_query import RedshiftLastQueryLoadErrorQuery
from ..postgresql.postgresql import PostgreSQLLayer, check_connection
from typing import Optional, Dict, List
from psycopg2 import InternalError as Psycopg2InternalError

class RedshiftLayer(PostgreSQLLayer):
  @check_connection
  def insert_data_frame(self, data_frame: pd.DataFrame, table_name: str, schema_name: Optional[str]=None, column_type_transform_dictionary: Optional[Dict[str, any]]=None, chunksize: int=1000, accept_invalid_characters: bool=False, empty_as_null: bool=False, transform_data_frame: bool=False):
    df = data_frame
    if column_type_transform_dictionary:
      if not transform_data_frame:
        df = data_frame.copy()
      for column, target_type in column_type_transform_dictionary.items():
        if column in df:
          print(f'converting column {column} to type {target_type}')
          df[column] = df[column].astype(target_type)
        else:
          print(f'column {column} not in data frame')

    pr.connect_to_s3(
      aws_access_key_id=self.connection_options.connector_options['aws_s3_access_key_id'],
      aws_secret_access_key=self.connection_options.connector_options['aws_s3_secret_access_key'],
      bucket=self.connection_options.connector_options['s3_bucket'],
      subdirectory=self.connection_options.connector_options['s3_bucket_directory']
    )

    should_connect = self.connection is None
    if should_connect:
      self.connect()
    try:
      parameters = []
      if accept_invalid_characters:
        parameters.append('ACCEPTINVCHARS')
      if empty_as_null:
        parameters.append('EMPTYASNULL')
      pr.pandas_to_redshift(
        data_frame=df,
        redshift_table_name=f'{schema_name}.{table_name}' if schema_name is not None else table_name,
        column_names_array=df.columns,
        region=self.connection_options.connector_options['s3_bucket_region'],
        append=True,
        connection=self.connection,
        parameters='\n'.join(parameters)
      )
    except Psycopg2InternalError as e:
      if e.pgerror and e.pgerror.find("Check 'stl_load_errors'") != -1:
        load_error = RedshiftLastQueryLoadErrorQuery().perform(sql_layer=self)
        if load_error:
          raise load_error
      raise e
    if should_connect:
      self.commit()
      self.disconnect()