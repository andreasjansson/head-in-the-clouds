from setuptools import setup

setup(name='headintheclouds',
      version='0.3.8',
      description='Provider-agnostic cloud provisioning and Docker orchestration',
      author='Andreas Jansson',
      author_email='andreas@jansson.me.uk',
      license='GNU GPLv3',
      packages=['headintheclouds', 'headintheclouds.ensemble'],
      url='https://github.com/andreasjansson/head-in-the-clouds',
      install_requires=[
          'Fabric==1.6.1',
          'PyYAML==3.10',
          'boto==2.9.8',
          'dop==0.1.4',
          'python-dateutil==2.1',
          'simplejson',
          'PyDbLite>=2.5.0',
      ],
      dependency_links = [
          'http://downloads.sourceforge.net/project/pydblite/pydblite/PyDbLite-2.5/PyDbLite-2.5.zip#egg=PyDbLite-2.5.0'
      ],
  )
