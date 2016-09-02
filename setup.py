from setuptools import setup, find_packages
from codecs import open as codecs_open

# Get the long description from the relevant file
with codecs_open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(name='pg_dependences',
      version='0.1',
      url='https://github.com/LionelR/pg_dependences',
      license='MIT',
      author='Lionel Roubeyrie',
      author_email='lionel dot roubeyrie at codinux dot fr',
      description='Summary report or cascaded graph dependencies for PostgreSQL objects',
      long_description=long_description,
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'click',
          'tabulate',
          'psycopg2',
          'graphviz'
      ],
      entry_points="""
          [console_scripts]
          pg_dependences=pg_dependences.pg_dependences:run
          """
      )
