Providers
=========

At the moment headintheclouds supports

* EC2
* Digital Ocean
* "Unmanaged" servers (machines you provision yourself, e.g. bare metal boxes)

In order for commands like ``fab nodes`` to list servers for a specific provider you need to import the cloud providers you plan to use::

   # fabfile.py
   from headintheclouds.tasks import *
   from headintheclouds import ec2
   from headintheclouds import digitalocean
   from headintheclouds import unmanaged

Provider-specific setup
-----------------------

EC2
~~~

To manage EC2 servers you need to define the following environment variables:

* ``AWS_ACCESS_KEY_ID``
* ``AWS_SECRET_ACCESS_KEY``
* ``AWS_SSH_KEY_FILENAME``
* ``AWS_KEYPAIR_NAME``

Digital Ocean
~~~~~~~~~~~~~

To manage EC2 servers you need these environment variables:

* ``DIGITAL_OCEAN_CLIENT_ID``
* ``DIGITAL_OCEAN_API_KEY``
* ``DIGITAL_OCEAN_SSH_KEY_FILENAME``
* ``DIGITAL_OCEAN_SSH_KEY_NAME``

Unmanaged servers
~~~~~~~~~~~~~~~~~

You can't really "manage" unmanaged servers, but in order to be able to log in to them and run commands, you may need to define

* ``HITC_SSH_USER`` (defaults to ``root``)
* ``HITC_KEY_FILENAME`` (defaults to ``~/.ssh/id_rsa``)

Since headintheclouds has no way of finding out which servers it doesn't manage, you should put the public ips/hostnames of your servers in a file called ``unmanaged_servers.txt`` in the same directory as your fabfile. The servers should be one per line, e.g.

::

   116.152.12.61
   116.152.12.62
   116.152.19.17
