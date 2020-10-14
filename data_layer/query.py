from datetime import datetime
from typing import Dict, Tuple, Optional, List, Generic, TypeVar

class SQLQuery:
  query: str
  substitution_parameters: Tuple[any]

  def __init__(self, query: str, substitution_parameters: Tuple[any] = (), multi: bool=False):
    self.query = query
    self.substitution_parameters = substitution_parameters

  @staticmethod
  def format_date(date: datetime) -> str:
    return date.strftime('%Y-%m-%d')

  @staticmethod
  def format_time(time: datetime) -> str:
    return time.strftime('%Y-%m-%d %H:%M:%S.%f')

  @staticmethod
  def format_array(array: List[any]) -> str:
    return '(' + ', '.join(['%s'] * len(array)) + ')'

  @staticmethod
  def escaped_query_text(query_text: str) -> str:
    return query_text.replace('%', '%%')

  @staticmethod
  def formatted_query_text(query_text: str, format_parameters: Dict[str, any]) -> str:
    return query_text.format(**format_parameters)

  @classmethod
  def array_condition_query(cls, expression: str, values: List[Optional[any]], negate: bool=False) -> 'SQLQuery':
    if not values:
      return cls(query='FALSE') if negate else cls(query='')

    not_modifier = 'NOT ' if negate else ''
    conditions = []
    if None in values:
      conditions.append(f'{expression} IS {not_modifier}NULL')
      substitution_parameters = tuple([v for v in values if v is not None])
    else:
      substitution_parameters = tuple(values)

    if substitution_parameters:
      conditions.append(f'{expression} {not_modifier}IN {cls.format_array(list(substitution_parameters))}')
    
    if len(conditions) > 1:
      logical_operator = ' AND ' if negate else ' OR '
      return cls(query='(' + logical_operator.join(conditions) + ')', substitution_parameters=substitution_parameters)
    else:
      return cls(query=conditions[0], substitution_parameters=substitution_parameters)
  
  @property
  def substituted_query(self) -> str:
    single_quote = "'"
    parameters = tuple(f"'{str(p).replace(single_quote, single_quote * 2)}'" for p in self.substitution_parameters)
    return self.query % parameters

  def run(self, sql_layer: 'SQLLayer') -> any:
    return sql_layer.query(query=self.query, substitution_parameters=self.substitution_parameters)

class GeneratedQuery(SQLQuery):
  def __init__(self):
    super().__init__(query='')
    self.generate_query()

  def generate_query(self):
    pass

class LiteralQuery(GeneratedQuery):
  literalValue: any

  def __init__(self, literalValue: any):
    self.literalValue = literalValue
    super().__init__()

  def generate_query(self):
    self.query = '%s'
    self.substitution_parameters = (self.literalValue,)

T = TypeVar(any)
class ResultQuery(Generic[T], SQLQuery):
  default_layer_type: any = None

  @property
  def layer_type(self) -> any:
    if self.default_layer_type is None:
      raise NotImplementedError()
    return self.default_layer_type

  def perform(self, sql_layer: 'SQLLayer') -> Optional[T]:
    cursor = self.run(sql_layer=sql_layer)
    return self.cursor_to_result(cursor=cursor)
  
  def cursor_to_result(self, cursor: any) -> Optional[T]:
    return cursor.fetchall()

  def get_result(self, layer_type: Optional[any]=None) -> Optional[T]:
    if layer_type is None:
      layer_type = self.layer_type
    layer = layer_type()
    layer.connect()
    result = self.perform(sql_layer=layer)
    layer.disconnect()
    return result

