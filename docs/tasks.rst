Tasks
=====

All tasks are executed from the command line as

::

   fab TASK_NAME:ARGUMENT_1,ARGUMENT_2,ARG_NAME_1=ARG_VALUE_1

To only execute the task on specific servers, use

::

   fab -H PUBLIC_IP_ADDRESS TASK_NAME

to run the task on that one server, or

::

   fab -R NAME TASK_NAME

to run the task on all servers with name ``NAME``. This includes servers with names like ``NAME-1``, ``NAME-2``, etc.

Global tasks
------------

.. automodule:: headintheclouds.tasks
   :members: nodes, create, terminate, reboot, rename, uncache, ssh, upload, pricing


Provider-specific create flags
------------------------------

EC2
~~~

* size='m1.small': See ``fab pricing`` for details
* placment='us-east-1b'
* bid=None: Define this to make spot requests
* image='ubuntu 14.04': Either an AMI ID or a shorthand Ubuntu version. The defined shorthands are 'ubuntu 14.04', 'ubuntu 14.04 ebs', 'ubuntu 14.04 hvm', where no 'ebs' or 'hvm' suffix indicate instance backing.
* security_group='default'

Digital Ocean
~~~~~~~~~~~~~

* size='512MB': Can be 512MB, 1GB, 2GB, [...], 96GB. See ``fab pricing`` for details
* placement='New York 1': Any Digital Ocean region, e.g. 'Singapore 1', 'Amsterdam 2'
* image='Ubuntu 14.04 x64': Can be any Digital Ocean image name, e.g. 'Ubuntu 14.04 x64', 'Fedora 19 x64', 'Arch Linux 2013.05 x64', etc.

Caching
-------

headintheclouds caches some data in `PyDbLite <http://www.pydblite.net/en/index.html>`_, most importantly the list of active nodes. This is so that calls like ``fab ssh`` doesn't take several seconds to run before actually logging in. It's possible to get into weird situations when other users create servers and you have the old cache. To flush the cache you can run ``fab uncache``. ``fab nodes`` and ``fab ensemble.up`` both flush the cache indirectly.

Namespacing
-----------

By default, all cloud servers created by headintheclouds will have their names prefixed by HITC-. This is so that headintheclouds-managed infrastructure doesn't interfere with other servers you might have. You can change this prefix by putting the line

::

   env.name_prefix = 'MYPREFIX-'

after you import ``*`` from ``fabric.api``, but **before importing headintheclouds**. So, for example

::

   from fabric.api import *

   env.name_prefix = 'INFRA-'

   from headintheclouds import ec2
   from headintheclouds import digitalocean
   from headintheclouds import unmanaged
   from headintheclouds import docker
   from headintheclouds import ensemble
   from headintheclouds.tasks import *
