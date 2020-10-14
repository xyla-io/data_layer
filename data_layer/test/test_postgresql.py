import pytest
import sqlalchemy as alchemy
import pandas as pd

from .. import PostgreSQLLayer
from config import postgresql_config
from sqlalchemy.ext.declarative import declarative_base

@pytest.fixture
def postgresql_layer():
  PostgreSQLLayer.configure_connection(options=postgresql_config)
  return PostgreSQLLayer()

def test_postgresql_connection(postgresql_layer):
  assert postgresql_layer is not None
  postgresql_layer.connect()
  postgresql_layer.disconnect()

def test_postgresql_query(postgresql_layer):
  postgresql_layer.connect()
  cursor = postgresql_layer.query('select * from dev1.tag_campaigns')
  rows = cursor.fetchall()
  assert cursor.rowcount > 0
  assert len(rows) == cursor.rowcount
  postgresql_layer.disconnect()

def test_postgresql_alchemy(postgresql_layer):
  session = postgresql_layer.alchemy_session()
  base = declarative_base(metadata=alchemy.MetaData(schema='dev1'))
  table = alchemy.Table(
    'tag_campaigns',
    base.metadata,
    alchemy.Column('campaign_id', alchemy.VARCHAR(255))
  )
  query = session.query(alchemy.func.count(table.columns.campaign_id))
  count = query.one()[0]
  assert count > 0
  session.close()

def test_postgresql_table_exists(postgresql_layer):
  postgresql_layer.connect()
  assert postgresql_layer.table_exists(table_name='tag_campaigns', schema_name='dev1')
  assert not postgresql_layer.table_exists(table_name='sassy_table', schema_name='dev1')
  assert not postgresql_layer.table_exists(table_name='tag_campaigns', schema_name='exploding_schema')
  postgresql_layer.disconnect()

def test_postgresql_insert(postgresql_layer):
  df = pd.DataFrame([
    {'a': 1, 'b': 2},
    {'a': 3, 'b': 4},
  ])
  postgresql_layer.insert_data_frame(data_frame=df, table_name='imploding_table', schema_name='dev1')