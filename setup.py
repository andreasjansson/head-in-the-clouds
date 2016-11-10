from setuptools import setup

setup(name='headintheclouds',
      version='0.8.2',
      description='Provider-agnostic cloud provisioning and Docker orchestration',
      author='Andreas Jansson',
      author_email='andreas@jansson.me.uk',
      license='GNU GPLv3',
      packages=['headintheclouds',
                'headintheclouds.ensemble',
                'headintheclouds.dependencies',
                'headintheclouds.dependencies.PyDbLite'],
      url='https://github.com/andreasjansson/head-in-the-clouds',
      install_requires=[
          'Fabric>=1.6.1',
          'PyYAML==3.12',
          'boto==2.9.8',
          'python-dateutil==2.1',
          'simplejson',
          'envtpl==0.3.2',
      ],
  )
