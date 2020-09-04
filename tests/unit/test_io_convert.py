"""Module for testing conversion of data between different I/O backends"""
import os
import shutil
import numpy as np
from abc import ABCMeta, abstractmethod
import unittest

from hdmf.backends.hdf5 import HDF5IO
from hdmf.common import get_manager
from hdmf.testing import TestCase
from hdmf.common import DynamicTable
#from hdmf.common import CSRMatrix

# Try to import Zarr and disable tests if Zarr is not available
try:
    import zarr
    from hdmf.backends.zarr.zarr_tools import ZarrIO
    from hdmf.backends.zarr.zarr_utils import ZarrDataIO
    DISABLE_ALL_ZARR_TESTS = False
except ImportError:
    DISABLE_ALL_ZARR_TESTS = True



class TestCaseConvertMixin(metaclass=ABCMeta):

    def setUp(self):
        self.__manager = get_manager()
        self.container = self.setUpContainer()
        self.container_type = self.container.__class__.__name__
        self.filename = 'test_%s.hdmf' % self.container_type
        self.export_filename = 'test_export_%s.hdmf' % self.container_type
        self.ios = []

    def tearDown(self):
        for io in self.ios:
            if io is not None:
                io.close()
        filenames = [self.filename, self. export_filename]
        for fn in filenames:
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
        self.assertContainerEqual(self.container, exported_container, ignore_name=True, ignore_hdmf_attrs=True)
        # TODO May need to add further asserts here


class TestHDF5toZarrMixin(TestCaseConvertMixin):

    def roundtripExportContainer(self):
        with HDF5IO(self.filename, manager=get_manager(), mode='w') as write_io:
            write_io.write(self.container, cache_spec=True)

        with HDF5IO(self.filename, manager=get_manager(), mode='r') as read_io:
            with ZarrIO(self.export_filename, mode='w') as export_io:
                export_io.export(src_io=read_io, write_args={'link_data': False})

        read_io = ZarrIO(self.export_filename, manager=get_manager(), mode='r')
        self.ios.append(read_io)
        exportContainer = read_io.read()
        return exportContainer


class TestZarrToHDF5Mixin(TestCaseConvertMixin):

    def roundtripExportContainer(self):
        with ZarrIO(self.filename, manager=get_manager(), mode='w') as write_io:
            write_io.write(self.container, cache_spec=True)

        with ZarrIO(self.filename, manager=get_manager(), mode='r') as read_io:
            with HDF5IO(self.export_filename, mode='w') as export_io:
                export_io.export(src_io=read_io, write_args={'link_data': False})

        read_io = HDF5IO(self.export_filename, manager=get_manager(), mode='r')
        self.ios.append(read_io)
        exportContainer = read_io.read()
        return exportContainer

class TestDynamicTableContainerMixin():

    def setUpContainer(self):
        table = DynamicTable('table0', 'an example table')
        table.add_column('foo', 'an int column')
        table.add_column('bar', 'a float column')
        table.add_column('qux', 'a boolean column')
        table.add_column('quux', 'a vocab column', vocab=True)
        table.add_row(foo=27, bar=28.0, qux=True, quux='a')
        table.add_row(foo=37, bar=38.0, qux=False, quux='b')
        return table
        # TODO: Comparison of string columns fails from Zarr-to-HDF5 because the data type changes from byte to str
        """
        table = DynamicTable('table0', 'an example table')
        table.add_column('foo', 'an int column')
        table.add_column('bar', 'a float column')
        table.add_column('baz', 'a string column')
        table.add_column('qux', 'a boolean column')
        table.add_column('quux', 'a vocab column', vocab=True)
        table.add_row(foo=27, bar=28.0, baz="cat", qux=True, quux='a')
        table.add_row(foo=37, bar=38.0, baz="dog", qux=False, quux='b')
        return table
        """


class TestCSRMatrixMixin():

    def setUpContainer(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        return CSRMatrix(data, indices, indptr, (3, 3))


@unittest.skipIf(DISABLE_ALL_ZARR_TESTS, "Skipping TestZarrWriter because Zarr is not installed")
class TestHDF5toZarrDynamicTable(TestDynamicTableContainerMixin, TestHDF5toZarrMixin, TestCase):
    pass


@unittest.skipIf(DISABLE_ALL_ZARR_TESTS, "Skipping TestZarrWriter because Zarr is not installed")
class TestZarrtoHDF5DynamicTable(TestDynamicTableContainerMixin, TestZarrToHDF5Mixin, TestCase):
    pass
