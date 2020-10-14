from .. import SQLQuery

def test_array_condition():
  query = SQLQuery.array_condition_query('table.column', [''])
  assert query.query == 'table.column IN (%s)'
  query = SQLQuery.array_condition_query('table.column', [None])
  assert query.query == 'table.column IS NULL'
  query = SQLQuery.array_condition_query('table.column', [None, ''])
  assert query.query == '(table.column IS NULL OR table.column IN (%s))'
  query = SQLQuery.array_condition_query('table.column', [''], negate=True)
  assert query.query == 'table.column NOT IN (%s)'
  query = SQLQuery.array_condition_query('table.column', [None], negate=True)
  assert query.query == 'table.column IS NOT NULL'
  query = SQLQuery.array_condition_query('table.column', [None, ''], negate=True)
  assert query.query == '(table.column IS NOT NULL AND table.column NOT IN (%s))'