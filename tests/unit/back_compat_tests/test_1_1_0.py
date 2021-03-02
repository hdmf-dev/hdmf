import os
from shutil import copyfile

from hdmf.backends.hdf5.h5tools import HDF5IO
from hdmf.testing import TestCase

from tests.unit.test_io_hdf5_h5tools import _get_manager
from tests.unit.utils import Foo, FooBucket


class Test1_1_0(TestCase):

    def setUp(self):
        # created using manager in test_io_hdf5_h5tools
        self.orig_1_0_5 = 'tests/unit/back_compat_tests/1.0.5.h5'
        self.path_1_0_5 = 'test_1.0.5.h5'
        copyfile(self.orig_1_0_5, self.path_1_0_5)

        # note: this may break if the current manager is different from the old manager
        # better to save a spec file
        self.manager = _get_manager()

    def tearDown(self):
        if os.path.exists(self.path_1_0_5):
            os.remove(self.path_1_0_5)

    def test_read_1_0_5(self):
        '''Test whether we can read files made by hdmf version 1.0.5'''
        with HDF5IO(self.path_1_0_5, manager=self.manager, mode='r') as io:
            read_foofile = io.read()
            self.assertTrue(len(read_foofile.buckets) == 1)
            self.assertListEqual(read_foofile.buckets['test_bucket'].foos['foo1'].my_data[:].tolist(), [0, 1, 2, 3, 4])
            self.assertListEqual(read_foofile.buckets['test_bucket'].foos['foo2'].my_data[:].tolist(), [5, 6, 7, 8, 9])

    def test_append_1_0_5(self):
        '''Test whether we can append to files made by hdmf version 1.0.5'''
        foo = Foo('foo3', [10, 20, 30, 40, 50], "I am foo3", 17, 3.14)
        foobucket = FooBucket('foobucket2', [foo])

        with HDF5IO(self.path_1_0_5, manager=self.manager, mode='a') as io:
            read_foofile = io.read()
            read_foofile.add_bucket(foobucket)
            io.write(read_foofile)

        with HDF5IO(self.path_1_0_5, manager=self.manager, mode='r') as io:
            read_foofile = io.read()
            self.assertListEqual(read_foofile.buckets['foobucket2'].foos['foo3'].my_data[:].tolist(), foo.my_data)
