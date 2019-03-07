.. _validating:

Validating HDMF data
====================

Validating HDMF structured data is is handled by a command-line tool available in :py:mod:`~hdmf`. The validator can be invoked like so:

.. code-block:: bash

    python -m hdmf.validate -p namespace.yaml test.h5

This will validate the file ``test.h5`` against the specification in ``namespace.yaml``.

