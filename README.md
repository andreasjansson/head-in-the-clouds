headintheclouds
===============

headintheclouds is a bunch of [Fabric](http://fabfile.org/) tasks for managing cloud servers and orchestrating Docker containers. Currently EC2 and Digital Ocean are supported.

**Full documentation here: [headintheclouds.readthedocs.org](http://headintheclouds.readthedocs.org)**

Install
-------

headintheclouds has been tested on Linux and OSX. Installation should be as simple as

    pip install headintheclouds

Tutorial
--------

In this tutorial we'll create a Wordpress server in EC2. First, create a new directory for the project. In that directory, create `fabfile.py` with the contents

    # fabfile.py
    from headintheclouds.tasks import *
    from headintheclouds import ec2
    from headintheclouds import ensemble
    from headintheclouds import docker

Define environment variables with your EC2 credentials:

    export AWS_ACCESS_KEY_ID=...
    export AWS_SECRET_ACCESS_KEY=...
    export AWS_SSH_KEY_FILENAME=...
    export AWS_KEYPAIR_NAME=...

On the command line, type

    fab nodes

The `nodes` task lists all the running nodes you've created. Since we haven't created any yet the output will look something like

    ec2 name  size  ip  internal_ip  state  created 


    Done.

In the same directory, create a file called `wordpress.yml` with the contents

    # wordpress.yml
    wordpress:
      provider: ec2
      image: ubuntu 14.04
      size: m1.small
      containers:
        wordpress:
          image: jbfink/docker-wordpress
          ports:
            - 80

On the command line, type

    fab ensemble.up:wordpress

The `ensemble` task figures out what needs to change in order to meet the wordpress.yml manifest. Since we don't have any servers yet, it will (with your permission) create a new m1.small server in EC2 and install Docker. Once that's done it will download and start the [jbfink/docker-wordpress](https://index.docker.io/u/jbfink/docker-wordpress/) Docker container, exposing port 80.

Now if we type `fab nodes` again, we'll see the new server running

    ec2 name       size      ip            internal_ip    state    created
    wordpress  m1.small  54.198.33.85  10.207.25.187  running  2014-03-16 17:21:41-04:00

We can see all the running docker processes with

    fab -R wordpress docker.ps

This will output

    [54.198.33.85] Executing task 'docker.ps'
    name       ip          ports           created              image                      
    wordpress  172.17.0.6  80:80, 22:None  2014-03-16 21:53:22  jbfink/docker-wordpress

If we open that IP (54.198.33.85 in this case) in a browser we see the Wordpress welcome page.

If we type `fab ensemble.up:wordpress` again, headintheclouds will realise that no changes need to be made and will just exit. We can kill the wordpress process with

    fab -R wordpress docker.kill:wordpress

Now if we do `fab ensemble.up:wordpress` it will only run the container but it won't start a new server.

That's pretty much it for a super basic tutorial. Let's kill the server

    fab -R wordpress terminate

Now `fab nodes` will be empty again.

A [more interesting Wordpress example](https://github.com/andreasjansson/head-in-the-clouds/blob/master/examples/wordpress.yml) can be found in the /examples directory.
