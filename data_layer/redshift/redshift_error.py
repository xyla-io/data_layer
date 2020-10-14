from ..error import SQLError

class RedshiftError(SQLError):
  pass

class RedshiftLoadError(RedshiftError):
  line_number: int
  column_name: str
  raw_field_value: str
  load_error_message: str

  def __init__(self, line_number: int, column_name: str, raw_field_value: str, load_error_message: str):
    self.line_number = line_number
    self.column_name = column_name
    self.raw_field_value = raw_field_value
    self.load_error_message = load_error_message
    super().__init__(self.message)

  @property
  def message(self) -> str:
    message = 'Load error'
    if self.line_number is not None:
      message += f' at line {self.line_number}'
    if self.column_name is not None:
      message += f' in column "{self.column_name}"'
    if self.raw_field_value is not None:
      message += f" for value '{self.raw_field_value}'"
    if self.load_error_message is not None:
      message += f': {self.load_error_message}'
    return message
