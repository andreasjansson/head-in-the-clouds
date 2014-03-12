from setuptools import setup

setup(name='headintheclouds',
      version='0.2.0',
      description='Provider-agnostic cloud provisioning and Docker orchestration',
      author='Andreas Jansson',
      author_email='andreas@jansson.me.uk',
      license='GNU GPLv3',
      packages=['headintheclouds'],
      url='https://github.com/andreasjansson/head-in-the-clouds',
      install_requires=[
          'Fabric==1.6.1',
          'PyYAML==3.10',
          'boto==2.9.8',
          'dop==0.1.4',
          'python-dateutil==2.1',
      ]
  )
