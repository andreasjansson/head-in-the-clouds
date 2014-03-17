Ensemble
========

This is really the sugar on the doughnut. headintheclouds.ensemble is an orchestration tool for Docker that will manage dependencies between containers, intelligently figure out what needs to change to meet the desired configuration, start servers and containers, and manage firewalls. It uses a simple YAML-based config format, and it's doing as much as possible in parallel.

I built ensemble on top of headintheclouds to manage `thisismyjam.com <http://www.thisismyjam.com`_. We're using it now for our production setup and it seems to hang together so far. The configuration format is heavily influenced by `Orchard's Fig http://orchardup.github.io/fig/`_.

Tasks
-----

.. automodule:: headintheclouds.ensemble
   :members: up

Configuration YAML schema
-------------------------

.. code-block:: yaml

   # The name of the server. If count > 1, the names will be
   # SERVER_NAME, SERVER_NAME-1, SERVER_NAME-2, [...].
   # If SERVER_NAME is an IP address, it is implied that it is
   # "unmanaged".
   SERVER_NAME:

     # An optional template, see below.
     template: TEMPLATE

     # Provider is required unless SERVER_NAME is an IP address.
     # Valid options are currently ec2 and digitalocean
     provider: PROVIDER

     # Optional. The number of copies of this server will
     # be created. Default=1.
     count: COUNT

     # Provider-specific settings, see the section on
     # provider-specific create flags in the Tasks section
     # Examples for an EC2 instance:
     size: m1.small
     image: ubuntu 12.04
     security_group: web_ssh

     # The containers to run
     containers:

       # The name of the container. Again, if container count > 1,
       # names will be suffixed with '', '-1', '-2', etc.
       CONTAINER_NAME:

         # Required. E.g. orchardup/redis.
         image: IMAGE

         # Optional. A list of ports to open in the format
         # CONTAINER_PORT[:EXPOSED_PORT][/PROTOCOL]
         ports:

           # Examples:
           - 80
           - 3306:3366
           - 1234/tcp
           - 1234:2345/udp

         # Optional hash of environment variables to pass to
         # docker run
         environment:

           # Optional template for env vars
           template: TEMPLATE
           
           # Examples:
           FOO: BAR
           hello: 123

         # Optional hash of volumes to bound mount in the
         # format HOST_DIR:CONTAINER_DIR
         volumes:

           # Examples:
           /docker-vol/web/tmp: /tmp
           /data/logs: /var/log

         # The number of instances of this container to run.
         # Default=1
         count: COUNT

     # Optional firewall configuration. If defined, only the
     # ports specified here will be open, all others will be
     # closed.
     firewall:

       # firewall also accepts an optional template
       template: TEMPLATE

       # The open ports are defined as a hash of PORT[/PROTOCOL]
       # to IP or list of IPs or "*" or $internal_ips, e.g.:
       3306: 10.1.1.12
       8125/udp: 10.1.1.15

       # "*" opens a port to the world
       22: "*"

       # $internal_ips is a special variable (see Variables and
       # dependencies below) that will expand to a list of all
       # internal IPs for the servers in the same configuration
       # file, effectively opening a port to all of them.
       6379: $internal_ips

       # Ports can also be wildcarded, like this
       "*/*": $internal_ips

   templates:
     TEMPLATE_NAME:
       # anything goes here
       
Templates
---------

To avoid having to write the same chunk of YAML over and over again, templates can be used as a sort of preprocessor macro. Anything that is defined in the main configuration will override the value in the template. For example, if you have a config that looks like this

.. code-block:: yaml

   myserver:
     template: foo
     containers:
       template: bar

   yourserver:
     template: foo
     containers:
       template: bar
       image: hello/world:other
       environment:
         template: baz

   templates:
     foo:
       provider: digitalocean
       size: 1GB
     bar:
       image: hello/world
       ports:
         - 80:9000
       environment:
         HELLO: 123
     baz:
       WORLD: 456

it will expand to

.. code-block:: yaml

   myserver:
     provider: digitalocean
     size: 1GB
     containers:
       image: hello/world
       ports:
         - 80:9000
       environment:
         HELLO: 123

   yourserver:
     provider: digitalocean
     size: 1GB
     containers:
       image: hello/world:other
       ports:
         - 80:9000
       environment:
         WORLD: 456


Variables and dependencies
--------------------------

Often you want to connect containers and servers, but you probably don't know the address of the server or container in advance. Enter variables and dependency management!

Here's an example:

.. code-block:: yaml

   web:
     provider: ec2
     containers:
       web:
         image: hello/web
       ports:
         - 80
       environment:
         REDIS_HOST: ${redis.ip}

   redis:
     provider: ec2
     containers:
       redis:
         image: orchardup/redis
       ports:
         - 6379

When you "up" this ensemble manifest from a vanilla setup with no running servers, the order of operations will be:

* Start "web" and "redis" servers in parallel
* Resolve ``${redis.ip}`` to the actual IP of the redis server
* Start the redis and web containers in parallel

If the web container would need to wait for the redis **container** to start, you could put in an environment variable like

.. code-block:: yaml

   # [snip]
       web:
         environment:
           REDIS_HOST: ${redis.ip}
           _DEPENDS: ${redis.containers.redis.ip}

headintheclouds.ensemble abstracts all the scheduling and will complain if you try to set up cyclical dependencies, so you can set up pretty complex dependency graphs without thinking too much about what's going on behind the scenes.

As a side note, headintheclouds doesn't use docker links, instead you point containers to the IPs of other servers and containers.

Idempotence and statelessness
-----------------------------

The only state that headintheclouds keeps is the internal caches, and these can be wiped without any negative side effects. Instead of storing state locally, the state of servers and containers is interrogated on the fly by logging in to the servers and checking what is actually running.

When you run ``fab ensemble.up:myensemble``, it will log in to any existing servers with the same names as in the manifest, and check if they're equivalent to what the configuration says. Then it will check the Docker containers and firewall rules on each host to see if they match the manifest.

This is how headintheclouds.ensemble is idempotent. You can run ``fab ensemble.up:myensemble`` any number of times with no effect on your servers, provided you don't change ``myensemble.yml``.

Before going out starting servers and containers, headintheclouds will prompt you to confirm the changes that will be made.

The only caveat is that headintheclouds doesn't currently delete servers and containers if you remove them from the manifest, you have to do that manually with the ``terminate`` and ``docker.kill`` commands. That's just so you don't go and tear things don't by accident.
