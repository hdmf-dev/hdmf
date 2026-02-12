import json
import os

from h5py import File
import yaml

from hdmf.backends.hdf5 import HDF5IO
from hdmf.common import Container, get_manager, get_hdf5io
from hdmf.spec import NamespaceCatalog
from hdmf.testing import TestCase, remove_test_file

from tests.unit.helpers.utils import get_temp_filepath


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

    def test_write_cache_spec_common_unresolved(self):
        """Test that cached specs match original, unresolved YAML files.

        This test verifies that the spec resolution system
        doesn't cache resolved specs with inherited fields.
        """
        with HDF5IO(self.path, manager=self.manager, mode="a") as io:
            io.write(self.container)

        # Get the path to the original YAML schema files
        import hdmf.common
        schema_dir = os.path.join(os.path.dirname(hdmf.common.__file__), 'hdmf-common-schema', 'common')

        # Get the namespace version
        ns = self.manager.namespace_catalog.get_namespace('hdmf-common')
        version = ns.version

        # All spec files in hdmf-common that are cached (excluding namespace.yaml and experimental.yaml)
        spec_files = ['base', 'resources', 'sparse', 'table']

        with File(self.path, 'r') as f:
            spec_group = f['specifications']['hdmf-common'][version]

            for spec_file in spec_files:
                with self.subTest(spec_file=spec_file):
                    cached_json = spec_group[spec_file][()].decode('utf-8')
                    cached = json.loads(cached_json)

                    yaml_path = os.path.join(schema_dir, f'{spec_file}.yaml')
                    with open(yaml_path, 'r') as yaml_file:
                        original = yaml.safe_load(yaml_file)

                    # Compare specs recursively (including subspecs)
                    self._compare_spec_dicts(cached, original, f'{spec_file}.yaml')

    def _compare_spec_dicts(self, cached: dict, original: dict, path: str) -> None:
        """Recursively compare cached and original spec dicts to check for extra resolved fields."""
        # Compare top-level datasets
        cached_datasets = cached.get('datasets', [])
        original_datasets = original.get('datasets', [])
        self.assertEqual(len(cached_datasets), len(original_datasets),
                         f"Dataset count mismatch at {path}")
        for i, (cached_ds, orig_ds) in enumerate(zip(cached_datasets, original_datasets)):
            ds_name = cached_ds.get('data_type_def') or cached_ds.get('name') or f'dataset[{i}]'
            ds_path = f"{path}/datasets/{ds_name}"
            self.assertEqual(set(cached_ds.keys()), set(orig_ds.keys()),
                             f"Keys mismatch at {ds_path}: cached={set(cached_ds.keys())}, "
                             f"original={set(orig_ds.keys())}")

        # Compare top-level groups and recurse into them
        cached_groups = cached.get('groups', [])
        original_groups = original.get('groups', [])
        self.assertEqual(len(cached_groups), len(original_groups),
                         f"Group count mismatch at {path}")
        for i, (cached_grp, orig_grp) in enumerate(zip(cached_groups, original_groups)):
            grp_name = cached_grp.get('data_type_def') or cached_grp.get('name') or f'group[{i}]'
            grp_path = f"{path}/groups/{grp_name}"
            self.assertEqual(set(cached_grp.keys()), set(orig_grp.keys()),
                             f"Keys mismatch at {grp_path}: cached={set(cached_grp.keys())}, "
                             f"original={set(orig_grp.keys())}")
            # Recursively check nested datasets and groups
            self._compare_spec_dicts(cached_grp, orig_grp, grp_path)


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


class TestGetHdf5IO(TestCase):

    def setUp(self):
        self.path = get_temp_filepath()

    def tearDown(self):
        remove_test_file(self.path)

    def test_gethdf5io(self):
        """Test the get_hdf5io convenience method with manager=None."""
        with get_hdf5io(self.path, "w") as io:
            self.assertIsNotNone(io.manager)

    def test_gethdf5io_manager(self):
        """Test the get_hdf5io convenience method with manager set."""
        manager = get_manager()
        with get_hdf5io(self.path, "w", manager=manager) as io:
            self.assertIs(io.manager, manager)
