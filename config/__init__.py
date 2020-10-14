import importlib

def import_config(name: str):
  try:
    module = importlib.import_module('config.local_{}_config'.format(name))
  except Exception:
    module = importlib.import_module('config.{}_config'.format(name))
  return getattr(module, '{}_config'.format(name))

postgresql_config = import_config('postgresql')