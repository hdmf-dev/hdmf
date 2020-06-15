import h5py
import os

from hdmf.backends import export_container, export_io
from hdmf.backends.hdf5.h5tools import HDF5IO
from hdmf.testing import TestCase

from tests.unit.test_io_hdf5_h5tools import _get_manager, FooFile
from tests.unit.utils import Foo, FooBucket, get_temp_filepath


class TestExportH5ToH5(TestCase):
    """Test exporting from an HDF5 file to an HDF5 file.

    See test_io_hdf5_h5tools.TestExport for more complete test cases since HDF5IO.export_container_to_hdf5 calls
    export_container.
    """

    def setUp(self):
        self.manager = _get_manager()
        self.path1 = get_temp_filepath()
        self.path2 = get_temp_filepath()
        self.path3 = get_temp_filepath()

    def tearDown(self):
        if os.path.exists(self.path1):
            os.remove(self.path1)
        if os.path.exists(self.path2):
            os.remove(self.path2)
        if os.path.exists(self.path3):
            os.remove(self.path3)

    def test_unwritten(self):
        """Test that exporting an unwritten container to file works without modifying container source."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('test_bucket', [foo1])
        foofile = FooFile([foobucket])

        export_container(
            container=foofile,
            write_io_cls=HDF5IO,
            type_map=self.manager.type_map,
            write_io_args={'path': self.path1, 'mode': 'w'}
        )

        self.assertTrue(os.path.exists(self.path1))
        self.assertIsNone(foofile.container_source)

        with HDF5IO(self.path1, manager=self.manager, mode='r') as io:
            read_foofile = io.read()
            self.assertEqual(read_foofile.container_source, self.path1)

            # containers should be equal after setting the container source of in-memory foofile
            foofile.container_source = self.path1
            self.assertContainerEqual(foofile, read_foofile)

    def test_written(self):
        """Test that exporting a written container works."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('test_bucket', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path1, manager=self.manager, mode='w') as io:
            io.write(foofile)

        with HDF5IO(self.path1, manager=self.manager, mode='r') as io:
            read_foofile = io.read()
            export_container(
                container=foofile,
                write_io_cls=HDF5IO,
                type_map=self.manager.type_map,
                write_io_args={'path': self.path2, 'mode': 'w'},
            )

        self.assertTrue(os.path.exists(self.path2))
        self.assertEqual(foofile.container_source, self.path1)
        self.assertEqual(read_foofile.container_source, self.path1)

        with HDF5IO(self.path2, manager=self.manager, mode='r') as io:
            read_foofile2 = io.read()
            self.assertEqual(read_foofile2.container_source, self.path2)
            self.assertContainerEqual(foofile, read_foofile, ignore_hdmf_attrs=True)

    def test_export_bad_write_cls(self):
        foofile = FooFile([])
        msg = "The 'write_io_cls' argument 'File' must be a subclass of HDMFIO."
        with self.assertRaisesWith(ValueError, msg):
            export_container(
                container=foofile,
                write_io_cls=h5py.File,
                type_map=self.manager.type_map,
                write_io_args={'path': self.path2, 'mode': 'w'},
            )

    def test_export_manager_arg(self):
        foofile = FooFile([])
        msg = "The 'manager' key is not allowed in write_io_args because a new BuildManager will be used."
        with self.assertRaisesWith(ValueError, msg):
            export_container(
                container=foofile,
                write_io_cls=HDF5IO,
                type_map=self.manager.type_map,
                write_io_args={'path': self.path2, 'mode': 'w', 'manager': self.manager},
            )

    def test_export_io_written(self):
        """Test that exporting a written container works."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('test_bucket', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path1, manager=self.manager, mode='w') as io:
            io.write(foofile)

        export_io(
            read_io_cls=HDF5IO,
            write_io_cls=HDF5IO,
            type_map=self.manager.type_map,
            read_io_args={'path': self.path1, 'mode': 'r', 'manager': self.manager},
            write_io_args={'path': self.path2, 'mode': 'w'},
        )

        self.assertTrue(os.path.exists(self.path2))
        self.assertEqual(foofile.container_source, self.path1)

        with HDF5IO(self.path2, manager=self.manager, mode='r') as io:
            read_foofile2 = io.read()
            self.assertEqual(read_foofile2.container_source, self.path2)
            self.assertContainerEqual(foofile, foofile, ignore_hdmf_attrs=True)

    def test_export_io_bad_read_cls(self):
        msg = "The 'read_io_cls' argument 'File' must be a subclass of HDMFIO."
        with self.assertRaisesWith(ValueError, msg):
            export_io(
                read_io_cls=h5py.File,
                write_io_cls=HDF5IO,
                type_map=self.manager.type_map,
                read_io_args={'path': self.path1, 'mode': 'r'},
                write_io_args={'path': self.path2, 'mode': 'w'},
            )
