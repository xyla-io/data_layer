from ..query import SQLQuery, GeneratedQuery, ResultQuery
from .redshift_error import RedshiftLoadError
from typing import Optional, Dict, List, TypeVar, Generic


class RedshiftQuery(SQLQuery):
  pass

class RedshiftMergeQuery(RedshiftQuery, GeneratedQuery):
  join_columns: List[str]
  update_columns: List[str]
  source_table: str
  target_table: str
  source_schema: str
  target_schema: str
  temp_table: str
  transaction: bool

  def __init__(self, join_columns: List[str], update_columns: List[str], source_table: str, target_table: str, source_schema: Optional[str]=None, target_schema: Optional[str]=None, temp_table: Optional[str]=None, transaction: bool=True):
    self.join_columns = join_columns
    self.update_columns = update_columns
    self.source_table = source_table
    self.target_table = target_table
    self.source_schema = '' if source_schema is None else f'{source_schema}.'
    self.target_schema = '' if target_schema is None else f'{target_schema}.'
    self.temp_table = f'merge_{source_table}_{target_table}' if temp_table is None else temp_table
    self.transaction = transaction
    super().__init__()

  def generate_query(self):
    set_statements = ',\n'.join([f'"{c}" = {self.temp_table}."{c}"' for c in self.update_columns])
    join_conditions = '\nand '.join([f'{self.target_schema}{self.target_table}."{c}" = {self.temp_table}."{c}"' for c in self.join_columns])
    start_transaction = '-- Start a new transaction\nbegin transaction;' if self.transaction else ''
    end_transaction = '-- End transaction and commit\nend transaction;' if self.transaction else ''

    self.query = f'''
-- Create a staging table and populate it with rows from the source table
create temp table {self.temp_table} as select * from {self.source_schema}{self.source_table};

{start_transaction}

-- Update the target table using an inner join with the staging table
update {self.target_schema}{self.target_table}
set {set_statements}
from {self.temp_table}
where {join_conditions};
 
-- Delete matching rows from the staging table 
-- using an inner join with the target table
delete from {self.temp_table}
using {self.target_schema}{self.target_table}
where {join_conditions};

-- Insert the remaining rows from the staging table into the target table
insert into {self.target_schema}{self.target_table}
select * from {self.temp_table};

{end_transaction}

-- Drop the staging table
drop table {self.temp_table};
    '''

class RedshiftMergeReplaceQuery(RedshiftQuery, GeneratedQuery):
  join_columns: List[str]
  source_table: str
  target_table: str
  source_schema: str
  target_schema: str
  transaction: bool

  def __init__(self, join_columns: List[str], source_table: str, target_table: str, source_schema: Optional[str]=None, target_schema: Optional[str]=None, transaction: bool=True):
    self.join_columns = join_columns
    self.source_table = source_table
    self.target_table = target_table
    self.source_schema = '' if source_schema is None else f'{source_schema}.'
    self.target_schema = '' if target_schema is None else f'{target_schema}.'
    self.transaction = transaction
    super().__init__()

  def generate_query(self):
    join_conditions = '\nand '.join([f'{self.target_schema}{self.target_table}."{c}" = {self.source_schema}{self.source_table}."{c}"' for c in self.join_columns])
    start_transaction = '-- Start a new transaction\nbegin transaction;' if self.transaction else ''
    end_transaction = '-- End transaction and commit\nend transaction;' if self.transaction else ''

    self.query = f'''
{start_transaction}

-- Delete matching rows from the target table 
-- using an inner join with the source table
delete from {self.target_schema}{self.target_table}
using {self.source_schema}{self.source_table}
where {join_conditions};

-- Insert the rows from the source table into the target table
insert into {self.target_schema}{self.target_table}
select * from {self.source_schema}{self.source_table};

{end_transaction}
    '''

T = TypeVar(any)
class RedshiftResultQuery(Generic[T], RedshiftQuery, ResultQuery[T]):
  pass

class RedshiftLastQueryIDQuery(RedshiftQuery, GeneratedQuery):
  def generate_query(self):
    self.query = 'select pg_last_copy_id()'

class RedshiftLastQueryLoadErrorQuery(GeneratedQuery, RedshiftResultQuery[RedshiftLoadError]):
  def generate_query(self):
    self.query = '''
select line_number, colname, raw_field_value, err_reason
from stl_load_errors
where query = pg_last_copy_id()
order by starttime desc
limit 1
    '''

  def cursor_to_result(self, cursor: any) -> Optional[RedshiftLoadError]:
    result = cursor.fetchone()
    if not result:
      return None
    return RedshiftLoadError(
      line_number=result[0],
      column_name=result[1].strip(),
      raw_field_value=result[2].strip(),
      load_error_message=result[3].strip()
    )