============================
How to Make a Roundtrip Test
============================

The HDMF test suite has tools for easily doing round-trip tests of container classes. These
tools exist in the :py:mod:`hdmf.testing` module. Round-trip tests exist for the container classes in the
:py:mod:`hdmf.common` module. We recommend you write any additional round-trip tests in
the ``tests/unit/common`` subdirectory of the Git repository.

For executing your new tests, we recommend using the `test.py` script in the top of the Git
repository. Roundtrip tests will get executed as part of the full test suite, which can be executed
with the following command::

    $ python test.py

The roundtrip test will generate a new HDMF file with the name ``test_<CLASS_NAME>.h5`` where ``CLASS_NAME`` is
the class name of the container class you are roundtripping. The test
will write an HDMF file with an instance of the container to disk, read this instance back in, and compare it
to the instance that was used for writing to disk. Once the test is complete, the HDMF file will be deleted.
You can keep the HDMF file around after the test completes by setting the environment variable ``CLEAN_HDMF``
to ``0``, ``false``, ``False``, or ``FALSE``. Setting ``CLEAN_HDMF`` to any value not listed here will
cause the roundtrip HDMF file to be deleted once the test has completed

Before writing tests, we also suggest you familiarize yourself with the
:ref:`software architecture <software-architecture>` of HDMF.

------------------------
``H5RoundTripMixin``
------------------------

To write a roundtrip test, you will need to subclass the
:py:class:`~hdmf.testing.testcase.H5RoundTripMixin` class and the
:py:class:`~hdmf.testing.testcase.TestCase` class, in that order, and override some of the instance methods of the
:py:class:`~hdmf.testing.testcase.H5RoundTripMixin` class to test the process of going from in-memory Python object
to data stored on disk and back.

##################
``setUpContainer``
##################

To configure the test for a particular container class, you need to override the
:py:meth:`~hdmf.testing.testcase.H5RoundTripMixin.setUpContainer` method. This method should take no arguments, and
return an instance of the container class you are testing.

Here is an example using a :py:class:`~hdmf.common.sparse.CSRMatrix`:

.. code-block:: python

    from hdmf.common import CSRMatrix
    from hdmf.testing import TestCase, H5RoundTripMixin
    import numpy as np

    class TestCSRMatrixRoundTrip(H5RoundTripMixin, TestCase):

        def setUpContainer(self):
            data = np.array([1, 2, 3, 4, 5, 6])
            indices = np.array([0, 2, 2, 0, 1, 2])
            indptr = np.array([0, 2, 3, 6])
            return CSRMatrix(data, indices, indptr, (3, 3))
