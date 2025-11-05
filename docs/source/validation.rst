.. _validating:

Validating HDMF Data
====================

Validation of NWB files is available through ``pynwb``. See the `PyNWB documentation
<https://pynwb.readthedocs.io/en/stable/validation.html>`_ for more information.

--------

.. note::

   A simple interface for validating HDMF structured data through the command line like for PyNWB files is not yet
   implemented. If you would like this functionality to be available through :py:mod:`~hdmf`, then please upvote
   `this issue <https://github.com/hdmf-dev/hdmf/issues/473>`_.

..
    Validating HDMF structured data is handled by a command-line tool available in :py:mod:`~hdmf`.
    The validator can be invoked like so:

    .. code-block:: bash

        python -m hdmf.validate -p namespace.yaml test.h5

    This will validate the file ``test.h5`` against the specification in ``namespace.yaml``.
