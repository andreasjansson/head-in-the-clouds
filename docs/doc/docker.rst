Docker
======

Working with private repositories
---------------------------------

If you want to run containers from private Docker repos, you will have to be signed in to that repo. Authentication sessions are stored in a file called ``~/.dockercfg`` and are created by ``docker login``.

headintheclouds can take care of most of this for you. If you create a file called ``dot_dockercfg`` that's a copy of your ``~/.dockercfg``, the ``fab docker.setup`` command will upload this file to the remote host as ``~/.dockercfg``.

Tasks
-----

.. automodule:: headintheclouds.docker
   :members: ssh, ps, bind, unbind, setup, run, kill, pull, inspect, tunnel

