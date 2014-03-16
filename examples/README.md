Head In The Clouds - Examples
=============================

These examples all run on EC2, so you need to define

    export AWS_ACCESS_KEY_ID=...
    export AWS_SECRET_ACCESS_KEY=...
    export AWS_SSH_KEY_FILENAME=...
    export AWS_KEYPAIR_NAME=...

To run an ensemble example, just type

    fab ensemble.up:MANIFEST_FILENAME

E.g.

    fab ensemble.up:wordpress
