from h5py import File

from hdmf.backends.hdf5 import HDF5IO
from hdmf.common import Container, get_manager
from hdmf.spec import NamespaceCatalog
from hdmf.testing import TestCase, remove_test_file

from tests.unit.utils import get_temp_filepath


class TestCacheSpec(TestCase):
    """Test caching spec specifically with the namespaces provided by hdmf.common.

    See also TestCacheSpec in tests/unit/test_io_hdf5_h5tools.py.
    """

    def setUp(self):
        self.manager = get_manager()
        self.path = get_temp_filepath()
        self.container = Container('dummy')

    def tearDown(self):
        remove_test_file(self.path)

    def test_write_no_cache_spec(self):
        """Roundtrip test for not writing spec."""
        with HDF5IO(self.path, manager=self.manager, mode="a") as io:
            io.write(self.container, cache_spec=False)
        with File(self.path, 'r') as f:
            self.assertNotIn('specifications', f)

    def test_write_cache_spec(self):
        """Roundtrip test for writing spec and reading it back in."""
        with HDF5IO(self.path, manager=self.manager, mode="a") as io:
            io.write(self.container)
        with File(self.path, 'r') as f:
            self.assertIn('specifications', f)
        self._check_spec()

    def test_write_cache_spec_injected(self):
        """Roundtrip test for writing spec and reading it back in when HDF5IO is passed an open h5py.File."""
        with File(self.path, 'w') as fil:
            with HDF5IO(self.path, manager=self.manager, file=fil, mode='a') as io:
                io.write(self.container)
        with File(self.path, 'r') as f:
            self.assertIn('specifications', f)
        self._check_spec()

    def _check_spec(self):
        ns_catalog = NamespaceCatalog()
        HDF5IO.load_namespaces(ns_catalog, self.path)
        self.maxDiff = None
        for namespace in self.manager.namespace_catalog.namespaces:
            with self.subTest(namespace=namespace):
                original_ns = self.manager.namespace_catalog.get_namespace(namespace)
                cached_ns = ns_catalog.get_namespace(namespace)
                ns_fields_to_check = list(original_ns.keys())
                ns_fields_to_check.remove('schema')  # schema fields will not match, so reset
                for ns_field in ns_fields_to_check:
                    with self.subTest(namespace_field=ns_field):
                        self.assertEqual(original_ns[ns_field], cached_ns[ns_field])
                for dt in original_ns.get_registered_types():
                    with self.subTest(data_type=dt):
                        original_spec = original_ns.get_spec(dt)
                        cached_spec = cached_ns.get_spec(dt)
                        with self.subTest('Data type spec is read back in'):
                            self.assertIsNotNone(cached_spec)
                        with self.subTest('Cached spec matches original spec'):
                            self.assertDictEqual(original_spec, cached_spec)
