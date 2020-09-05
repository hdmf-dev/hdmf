"""
Module for testing conversion of data between different I/O backends

To reduce the amount of code needed, the tests use a series of mixin classes to
construct a test case:

- TestCaseConvertMixin is the base mixin class for conversion tests and
  requires that the setUpContainer and roundtripExportContainer functions
  are defined. The setUpContainer defines the container (and hence the problem case)
  to be written to file. And the rountripExportContainer defined the process
  for writing, exporting, and then reading the container again.
- TestXYZContainerMixin classes define the setUpContainer function
- TestX1toX2Mixin defines the rountripExportContainer process
- TestCase is the base test class for HDMF

A test case is then constructed by defining a class that inherits from the
corresponding (usually 4) base classes, a mixin that define setUpContainer,
a mixin that define roundtripExportContainer, TestCaseConvertMixin, and TestCase.
I.e., even though a particular test class may look empty, it is the combination
of the mixin classes that creates the particular test problem.
"""
import os
import shutil
import numpy as np
from abc import ABCMeta, abstractmethod
import unittest

from hdmf.backends.hdf5 import HDF5IO
from hdmf.common import get_manager as get_hdmfcommon_manager
from hdmf.testing import TestCase
from hdmf.common import DynamicTable
from hdmf.common import CSRMatrix
from tests.unit.utils import (Foo, FooBucket, FooFile, get_foo_buildmanager)

# Try to import Zarr and disable tests if Zarr is not available
try:
    from hdmf.backends.zarr.zarr_tools import ZarrIO
    DISABLE_ALL_ZARR_TESTS = False
except ImportError:
    DISABLE_ALL_ZARR_TESTS = True


class TestCaseConvertMixin(metaclass=ABCMeta):
    """
    Mixin class used to define the basic structure for a conversion test.
    """
    IGNORE_NAME = False
    IGNORE_HDMF_ATTRS = False
    IGNORE_STRING_TO_BYTE = False

    def get_manager(self):
        raise NotImplementedError('Cannot run test unless get_manger  is implemented')

    def setUp(self):
        self.__manager = self.get_manager()
        self.container = self.setUpContainer()
        self.container_type = self.container.__class__.__name__
        self.filename = 'test_%s.hdmf' % self.container_type
        self.export_filename = 'test_export_%s.hdmf' % self.container_type
        self.filenames = [self.filename, self.export_filename]
        self.ios = []

    def tearDown(self):
        for io in self.ios:
            if io is not None:
                io.close()
        for fn in self.filenames:
            if fn is not None and os.path.exists(fn):
                if os.path.isdir(fn):
                    shutil.rmtree(fn)
                else:
                    os.remove(fn)

    @abstractmethod
    def setUpContainer(self):
        """Return the Container to read/write."""
        raise NotImplementedError('Cannot run test unless setUpContainer is implemented')

    @abstractmethod
    def roundtripExportContainer(self):
        """
        1. Write the container to self.filename
        2. Export the file from 1 to self.export_filename using a new backend
        3. Read the exported container from disk
        4. Return the container read in 4 so that it can be compared with the original

        Any HDMFIO backends that should remain open should be added to the self.io list
        so that they can be closed on tearDown.
        """
        raise NotImplementedError('Cannot run test unless roundtripExportContainer  is implemented')

    def test_export_roundtrip(self):
        """Test that rountripping the container works"""
        exported_container = self.roundtripExportContainer()
        self.assertIsNotNone(str(self.container))  # added as a test to make sure printing works
        self.assertIsNotNone(str(exported_container))
        # make sure we get a completely new object
        self.assertNotEqual(id(self.container), id(exported_container))
        # the name of the root container of a file is always 'root' (see h5tools.py ROOT_NAME)
        # thus, ignore the name of the container when comparing original container vs read container
        self.assertContainerEqual(self.container, exported_container,
                                  ignore_name=self.IGNORE_NAME,
                                  ignore_hdmf_attrs=self.IGNORE_HDMF_ATTRS,
                                  ignore_string_to_byte=self.IGNORE_STRING_TO_BYTE)
        # TODO May need to add further asserts here


############################################
# HDMF Common test harness
###########################################
class TestHDF5ToZarrMixin():
    """
    Mixin class used in conjunction with TestCaseConvertMixin to create conversion tests from HDF5 to Zarr.
    This class only defines the roundtripExportContainer and get_manager functions for the test.
    The setUpContainer function required for the test needs to be defined separately
    (e.g., by another mixin or the test class itself).
    """
    def get_manager(self):
        return get_hdmfcommon_manager()

    def roundtripExportContainer(self):
        with HDF5IO(self.filename, manager=self.get_manager(), mode='w') as write_io:
            write_io.write(self.container, cache_spec=True)

        with HDF5IO(self.filename, manager=self.get_manager(), mode='r') as read_io:
            with ZarrIO(self.export_filename, mode='w') as export_io:
                export_io.export(src_io=read_io, write_args={'link_data': False})

        read_io = ZarrIO(self.export_filename, manager=self.get_manager(), mode='r')
        self.ios.append(read_io)
        exportContainer = read_io.read()
        return exportContainer


class TestZarrToHDF5Mixin():
    """
    Mixin class used in conjunction with TestCaseConvertMixin to create conversion tests from Zarr to HDF5.
    This class only defines the roundtripExportContainer and get_manager functions for the test.
    The setUpContainer function required for the test needs to be defined separately
    (e.g., by another mixin or the test class itself)
    """
    def get_manager(self):
        return get_hdmfcommon_manager()

    def roundtripExportContainer(self):
        with ZarrIO(self.filename, manager=self.get_manager(), mode='w') as write_io:
            write_io.write(self.container, cache_spec=True)

        with ZarrIO(self.filename, manager=self.get_manager(), mode='r') as read_io:
            with HDF5IO(self.export_filename, mode='w') as export_io:
                export_io.export(src_io=read_io, write_args={'link_data': False})

        read_io = HDF5IO(self.export_filename, manager=self.get_manager(), mode='r')
        self.ios.append(read_io)
        exportContainer = read_io.read()
        return exportContainer


class TestDynamicTableContainerMixin():
    """
    Mixin class used in conjunction with TestCaseConvertMixin to create conversion tests that
    test export of DynamicTable container classes. This class only defines the setUpContainer function for the test.
    The roundtripExportContainer function required for the test needs to be defined separately
    (e.g., by another mixin or the test class itself)

    This mixin adds the class variable, TABLE_TYPE  which is an int to select between different
    container types for testing:

    TABLE_TYPE=0 : Table of int, float, bool, vocabulary
    TABLE_TYPE=1 : Table of int, float, str, bool, vocabulary
    """
    TABLE_TYPE = 0

    def setUpContainer(self):
        if self.TABLE_TYPE == 0:
            table = DynamicTable('table0', 'an example table')
            table.add_column('foo', 'an int column')
            table.add_column('bar', 'a float column')
            table.add_column('qux', 'a boolean column')
            table.add_column('quux', 'a vocab column', vocab=True)
            table.add_row(foo=27, bar=28.0, qux=True, quux='a')
            table.add_row(foo=37, bar=38.0, qux=False, quux='b')
            return table
        elif self.TABLE_TYPE == 1:
            table = DynamicTable('table0', 'an example table')
            table.add_column('foo', 'an int column')
            table.add_column('bar', 'a float column')
            table.add_column('baz', 'a string column')
            table.add_column('qux', 'a boolean column')
            table.add_column('quux', 'a vocab column', vocab=True)
            table.add_row(foo=27, bar=28.0, baz="cat", qux=True, quux='a')
            table.add_row(foo=37, bar=38.0, baz="dog", qux=False, quux='b')
            return table
        else:
            raise NotImplementedError("TABLE_TYPE %i not implemented in test" % self.TABLE_TYPE)


class TestCSRMatrixMixin():
    """
    Mixin class used in conjunction with TestCaseConvertMixin to create conversion tests that
    test export of CSRMatrix container classes. This class only defines the setUpContainer function for the test.
    The roundtripExportContainer function required for the test needs to be defined separately
    (e.g., by another mixin or the test class itself)
    """

    def setUpContainer(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        return CSRMatrix(data, indices, indptr, (3, 3))


#########################################
# HDMF Foo test container test harness
#########################################
class TestZarrToHDF5FooMixin(TestZarrToHDF5Mixin):
    """
    Convert mixin for Zarr to HDF5 but using the BuildManager for the Foo test containers
    """
    def get_manager(self):
        return get_foo_buildmanager()


class TestHDF5ToZarrFooMixin(TestHDF5ToZarrMixin):
    """
    Convert mixin for HDF5 to Zarr but using the BuildManager for the Foo test containers
    """
    def get_manager(self):
        return get_foo_buildmanager()


class TestFooMixin():
    """
    Mixin class used in conjunction with TestCaseConvertMixin to create conversion tests that
    test export of a variety of Foo container classes. This class only defines the setUpContainer
    function for the test. The roundtripExportContainer and get_manager function required for
    the test needs to be defined separately, e.g., by another mixin for Foo test cases, e.g.,
    TestZarrToHDF5FooMixin or TestHDF5ToZarrFooMixin.


    This mixin adds the class variable, FOO_TYPE  which is an int to select between different
    container types for testing:

    FOO_TYPE=0 : File with two Foo buckets storing integer datasets
    FOO_TYPE=1 : File with one Foo buckets storing integer dataset and a SoftLink to it
    """
    FOO_TYPE = 0
    FOO_TYPES = {'int_data': 0,
                 'link_data': 1,
                 'str_data': 2}

    def setUpContainer(self):
        if self.FOO_TYPE == 0:
            foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
            foo2 = Foo('foo2', [5, 6, 7, 8, 9], "I am foo2", 34, 6.28)
            foobucket = FooBucket('bucket1', [foo1, foo2])
            foofile = FooFile(buckets=[foobucket])
            return foofile
        elif self.FOO_TYPE == 1:
            foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
            foobucket = FooBucket('bucket1', [foo1])
            foofile = FooFile(buckets=[foobucket], foo_link=foo1)  # create soft link
            return foofile
        else:
            raise NotImplementedError("FOO_TYPE %i not implemented in test" % self.FOO_TYPE)


########################################
# Actual test cases for conversion
########################################
@unittest.skipIf(DISABLE_ALL_ZARR_TESTS, "Skipping TestZarrWriter because Zarr is not installed")
class TestHDF5ToZarrDynamicTableC0(TestDynamicTableContainerMixin,
                                   TestHDF5ToZarrMixin,
                                   TestCaseConvertMixin,
                                   TestCase):
    """
    Test the conversion of DynamicTable containers from HDF5 to Zarr.
    See TestDynamicTableContainerMixin.setUpContainer for the container spec.
    """
    IGNORE_NAME = True
    IGNORE_HDMF_ATTRS = True
    IGNORE_STRING_TO_BYTE = False
    TABLE_TYPE = 0


@unittest.skipIf(DISABLE_ALL_ZARR_TESTS, "Skipping TestZarrWriter because Zarr is not installed")
class TestZarrToHDF5DynamicTableC0(TestDynamicTableContainerMixin,
                                   TestZarrToHDF5Mixin,
                                   TestCaseConvertMixin,
                                   TestCase):
    """
    Test the conversion of DynamicTable containers from Zarr to HDF5.
    See TestDynamicTableContainerMixin.setUpContainer for the container spec.
    """
    IGNORE_NAME = True
    IGNORE_HDMF_ATTRS = True
    IGNORE_STRING_TO_BYTE = False
    TABLE_TYPE = 0


@unittest.skipIf(DISABLE_ALL_ZARR_TESTS, "Skipping TestZarrWriter because Zarr is not installed")
class TestHDF5ToZarrDynamicTableC1(TestDynamicTableContainerMixin,
                                   TestHDF5ToZarrMixin,
                                   TestCaseConvertMixin,
                                   TestCase):
    """
    Test the conversion of DynamicTable containers from HDF5 to Zarr.
    See TestDynamicTableContainerMixin.setUpContainer for the container spec.
    """
    IGNORE_NAME = True
    IGNORE_HDMF_ATTRS = True
    IGNORE_STRING_TO_BYTE = False
    TABLE_TYPE = 1


@unittest.skipIf(DISABLE_ALL_ZARR_TESTS, "Skipping TestZarrWriter because Zarr is not installed")
class TestZarrToHDF5DynamicTableC1(TestDynamicTableContainerMixin,
                                   TestZarrToHDF5Mixin,
                                   TestCaseConvertMixin,
                                   TestCase):
    """
    Test the conversion of DynamicTable containers from Zarr to HDF5.
    See TestDynamicTableContainerMixin.setUpContainer for the container spec.
    """
    IGNORE_NAME = True
    IGNORE_HDMF_ATTRS = True
    IGNORE_STRING_TO_BYTE = True   # Need to ignore conversion of strings to bytes
    TABLE_TYPE = 1


@unittest.skipIf(DISABLE_ALL_ZARR_TESTS, "Skipping TestZarrWriter because Zarr is not installed")
class TestHDF5ToZarrCSRMatrix(TestCSRMatrixMixin,
                              TestHDF5ToZarrMixin,
                              TestCaseConvertMixin,
                              TestCase):
    """
    Test the conversion of CSRMatrix containers from HDF5 to Zarr.
    """
    IGNORE_NAME = True
    IGNORE_HDMF_ATTRS = True
    IGNORE_STRING_TO_BYTE = False


@unittest.skipIf(DISABLE_ALL_ZARR_TESTS, "Skipping TestZarrWriter because Zarr is not installed")
class TestZarrToHDF5CSRMatrix(TestCSRMatrixMixin,
                              TestZarrToHDF5Mixin,
                              TestCaseConvertMixin,
                              TestCase):
    """
    Test the conversion of CSRMatrix containers from Zarr to HDF5.
    """
    IGNORE_NAME = True
    IGNORE_HDMF_ATTRS = True
    IGNORE_STRING_TO_BYTE = False


@unittest.skipIf(DISABLE_ALL_ZARR_TESTS, "Skipping TestZarrWriter because Zarr is not installed")
class TestZarrToHDF5FooCase1(TestFooMixin,
                             TestZarrToHDF5FooMixin,
                             TestCaseConvertMixin,
                             TestCase):
    """
    Test the conversion of a simple Foo container with two buckets of datasets from Zarr to HDF5
    See TestFooMixin.setUpContainer for the container spec used.
    """
    IGNORE_NAME = True
    IGNORE_HDMF_ATTRS = True
    IGNORE_STRING_TO_BYTE = True
    FOO_TYPE = TestFooMixin.FOO_TYPES['int_data']


@unittest.skipIf(DISABLE_ALL_ZARR_TESTS, "Skipping TestZarrWriter because Zarr is not installed")
class TestHDF5toZarrFooCase1(TestFooMixin,
                             TestHDF5ToZarrFooMixin,
                             TestCaseConvertMixin,
                             TestCase):
    """
    Test the conversion of a simple Foo container with two buckets of datasets from Zarr to HDF5
    See TestFooMixin.setUpContainer for the container spec used.
    """
    IGNORE_NAME = True
    IGNORE_HDMF_ATTRS = True
    IGNORE_STRING_TO_BYTE = True
    FOO_TYPE = TestFooMixin.FOO_TYPES['int_data']


@unittest.skipIf(DISABLE_ALL_ZARR_TESTS, "Skipping TestZarrWriter because Zarr is not installed")
class TestZarrToHDF5FooCase2(TestFooMixin,
                             TestZarrToHDF5FooMixin,
                             TestCaseConvertMixin,
                             TestCase):
    """
    Test the conversion of a simple Foo container with two buckets of datasets from Zarr to HDF5
    See TestFooMixin.setUpContainer for the container spec used.
    """
    IGNORE_NAME = True
    IGNORE_HDMF_ATTRS = True
    IGNORE_STRING_TO_BYTE = True
    FOO_TYPE = TestFooMixin.FOO_TYPES['link_data']


@unittest.skipIf(DISABLE_ALL_ZARR_TESTS, "Skipping TestZarrWriter because Zarr is not installed")
class TestHDF5toZarrFooCase2(TestFooMixin,
                             TestHDF5ToZarrFooMixin,
                             TestCaseConvertMixin,
                             TestCase):
    """
    Test the conversion of a simple Foo container with two buckets of datasets from Zarr to HDF5
    See TestFooMixin.setUpContainer for the container spec used.
    """
    IGNORE_NAME = True
    IGNORE_HDMF_ATTRS = True
    IGNORE_STRING_TO_BYTE = True
    FOO_TYPE = TestFooMixin.FOO_TYPES['link_data']


@unittest.skipIf(DISABLE_ALL_ZARR_TESTS, "Skipping TestZarrWriter because Zarr is not installed")
class TestFooExternalLinkHDF5ToZarr(TestCaseConvertMixin, TestCase):

    IGNORE_NAME = True
    IGNORE_HDMF_ATTRS = True
    IGNORE_STRING_TO_BYTE = False

    def get_manager(self):
        return get_foo_buildmanager()

    def setUpContainer(self):
        # Create the first file container. We will overwrite this later with the external link container
        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        bucket1 = FooBucket('bucket1', [foo1])
        foofile1 = FooFile(buckets=[bucket1])
        return foofile1

    def roundtripExportContainer(self):
        # Write the HDF5 file
        first_filename = 'test_firstfile_%s.hdmf' % self.container_type
        self.filenames.append(first_filename)
        with HDF5IO(first_filename, manager=self.get_manager(), mode='w') as write_io:
            write_io.write(self.container, cache_spec=True)

        # Create the second file with an external link added (which is the file we use as reference_
        with HDF5IO(first_filename, manager=self.get_manager(), mode='r') as read_io:
            read_foo = read_io.read()
            foo2 = Foo('foo2', read_foo.buckets['bucket1'].foos['foo1'].my_data, "I am foo2", 34, 6.28)
            bucket2 = FooBucket('bucket2', [foo2])
            foofile2 = FooFile(buckets=[bucket2])
            self.container = foofile2  # This is what we need to compare against
            with HDF5IO(self.filename, manager=self.get_manager(), mode='w') as write_io:
                write_io.write(foofile2, cache_spec=True)

        # Export the file with the external link to Zarr
        with HDF5IO(self.filename, manager=self.get_manager(), mode='r') as read_io:
            with ZarrIO(self.export_filename, mode='w') as export_io:
                export_io.export(src_io=read_io, write_args={'link_data': False})

        read_io = ZarrIO(self.export_filename, manager=self.get_manager(), mode='r')
        self.ios.append(read_io)
        exportContainer = read_io.read()
        return exportContainer
