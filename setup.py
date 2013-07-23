from setuptools import setup

setup(name='headintheclouds',
      version='0.1',
      description='Provider-agnostic cloud computing on the cheap',
      author='Andreas Jansson',
      author_email='andreas@jansson.me.uk',
      licence='GNU GPLv3',
      packages=['headintheclouds'],
      requires=[
          'fabric',
          'pyyaml',
          'boto',
          'numpy',
          'dop',
          'pyfscache',
      ]
  )
