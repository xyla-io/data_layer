from setuptools import setup, find_packages

setup(name='data_layer',
      version='0.0.1',
      description='Xyla\'s database layer.',
      url='https://github.com/xyla-io/data_layer',
      author='Xyla',
      author_email='gklei89@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[
        'sqlalchemy',
        'pandas',
        'pytest',
        'psycopg2-binary',
        'boto3',
      ],
      zip_safe=False)
