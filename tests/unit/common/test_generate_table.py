import numpy as np
import os
import shutil
import tempfile

from hdmf.backends.hdf5 import HDF5IO
from hdmf.build import BuildManager, TypeMap
from hdmf.common import get_type_map, DynamicTable
from hdmf.spec import GroupSpec, DatasetSpec, SpecCatalog, SpecNamespace, NamespaceCatalog
from hdmf.testing import TestCase
from hdmf.validate import ValidatorMap

from tests.unit.utils import CORE_NAMESPACE


class TestDynamicDynamicTable(TestCase):

    def setUp(self):
        self.dt_spec = GroupSpec(
            'A test extension that contains a dynamic table',
            data_type_def='TestTable',
            data_type_inc='DynamicTable',
            datasets=[
                DatasetSpec(
                    data_type_inc='VectorData',
                    name='my_col',
                    doc='a test column',
                    dtype='float'
                ),
                DatasetSpec(
                    data_type_inc='VectorData',
                    name='indexed_col',
                    doc='a test column',
                    dtype='float'
                ),
                DatasetSpec(
                    data_type_inc='VectorIndex',
                    name='indexed_col_index',
                    doc='a test column',
                ),
                DatasetSpec(
                    data_type_inc='VectorData',
                    name='optional_col1',
                    doc='a test column',
                    dtype='float',
                    quantity='?',
                ),
                DatasetSpec(
                    data_type_inc='VectorData',
                    name='optional_col2',
                    doc='a test column',
                    dtype='float',
                    quantity='?',
                )
            ]
        )

        self.dt_spec2 = GroupSpec(
            'A test extension that contains a dynamic table',
            data_type_def='TestDTRTable',
            data_type_inc='DynamicTable',
            datasets=[
                DatasetSpec(
                    data_type_inc='DynamicTableRegion',
                    name='ref_col',
                    doc='a test column',
                ),
                DatasetSpec(
                    data_type_inc='DynamicTableRegion',
                    name='indexed_ref_col',
                    doc='a test column',
                ),
                DatasetSpec(
                    data_type_inc='VectorIndex',
                    name='indexed_ref_col_index',
                    doc='a test column',
                ),
                DatasetSpec(
                    data_type_inc='DynamicTableRegion',
                    name='optional_ref_col',
                    doc='a test column',
                    quantity='?'
                ),
                DatasetSpec(
                    data_type_inc='DynamicTableRegion',
                    name='optional_indexed_ref_col',
                    doc='a test column',
                    quantity='?'
                ),
                DatasetSpec(
                    data_type_inc='VectorIndex',
                    name='optional_indexed_ref_col_index',
                    doc='a test column',
                    quantity='?'
                ),
                DatasetSpec(
                    data_type_inc='VectorData',
                    name='optional_col3',
                    doc='a test column',
                    dtype='float',
                    quantity='?',
                )
            ]
        )

        from hdmf.spec.write import YAMLSpecWriter
        writer = YAMLSpecWriter(outdir='.')

        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.dt_spec, 'test.yaml')
        self.spec_catalog.register_spec(self.dt_spec2, 'test.yaml')
        self.namespace = SpecNamespace(
            'a test namespace', CORE_NAMESPACE,
            [
                dict(
                    namespace='hdmf-common',
                ),
                dict(source='test.yaml'),
            ],
            version='0.1.0',
            catalog=self.spec_catalog
        )

        self.test_dir = tempfile.mkdtemp()
        spec_fpath = os.path.join(self.test_dir, 'test.yaml')
        namespace_fpath = os.path.join(self.test_dir, 'test-namespace.yaml')
        writer.write_spec(dict(groups=[self.dt_spec, self.dt_spec2]), spec_fpath)
        writer.write_namespace(self.namespace, namespace_fpath)
        self.namespace_catalog = NamespaceCatalog()
        hdmf_typemap = get_type_map()
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.merge(hdmf_typemap, ns_catalog=True)
        self.type_map.load_namespaces(namespace_fpath)
        self.manager = BuildManager(self.type_map)

        self.TestTable = self.type_map.get_dt_container_cls('TestTable', CORE_NAMESPACE)
        self.TestDTRTable = self.type_map.get_dt_container_cls('TestDTRTable', CORE_NAMESPACE)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir)

    def test_dynamic_table(self):
        assert issubclass(self.TestTable, DynamicTable)

        assert self.TestTable.__columns__[0] == dict(
            name='my_col',
            description='a test column',
            required=True
        )

    def test_forbids_incorrect_col(self):
        test_table = self.TestTable(name='test_table', description='my test table')

        with self.assertRaises(ValueError):
            test_table.add_row(my_col=3.0, indexed_col=[1.0, 3.0], incorrect_col=5)

    def test_dynamic_column(self):
        test_table = self.TestTable(name='test_table', description='my test table')
        test_table.add_column('dynamic_column', 'this is a dynamic column')
        test_table.add_row(
            my_col=3.0, indexed_col=[1.0, 3.0], dynamic_column=4, optional_col2=.5,
        )
        test_table.add_row(
            my_col=4.0, indexed_col=[2.0, 4.0], dynamic_column=4, optional_col2=.5,
        )

        np.testing.assert_array_equal(test_table['indexed_col'].target.data, [1., 3., 2., 4.])
        np.testing.assert_array_equal(test_table['dynamic_column'].data, [4, 4])

    def test_optional_col(self):
        test_table = self.TestTable(name='test_table', description='my test table')
        test_table.add_row(my_col=3.0, indexed_col=[1.0, 3.0], optional_col2=.5)
        test_table.add_row(my_col=4.0, indexed_col=[2.0, 4.0], optional_col2=.5)

    def test_dynamic_table_region(self):
        test_table = self.TestTable(name='test_table', description='my test table')
        test_table.add_row(my_col=3.0, indexed_col=[1.0, 3.0], optional_col2=.5)
        test_table.add_row(my_col=4.0, indexed_col=[2.0, 4.0], optional_col2=.5)

        test_dtr_table = self.TestDTRTable(name='test_dtr_table', description='my table',
                                           target_tables={'ref_col': test_table,
                                                          'indexed_ref_col': test_table})
        self.assertIs(test_dtr_table['ref_col'].table, test_table)
        self.assertIs(test_dtr_table['indexed_ref_col'].target.table, test_table)

        test_dtr_table.add_row(ref_col=0, indexed_ref_col=[0, 1])
        test_dtr_table.add_row(ref_col=0, indexed_ref_col=[0, 1])

        np.testing.assert_array_equal(test_dtr_table['indexed_ref_col'].target.data, [0, 1, 0, 1])
        np.testing.assert_array_equal(test_dtr_table['ref_col'].data, [0, 0])

    def test_dynamic_table_region_optional(self):
        test_table = self.TestTable(name='test_table', description='my test table')
        test_table.add_row(my_col=3.0, indexed_col=[1.0, 3.0], optional_col2=.5)
        test_table.add_row(my_col=4.0, indexed_col=[2.0, 4.0], optional_col2=.5)

        test_dtr_table = self.TestDTRTable(name='test_dtr_table', description='my table',
                                           target_tables={'optional_ref_col': test_table,
                                                          'optional_indexed_ref_col': test_table})
        self.assertIs(test_dtr_table['optional_ref_col'].table, test_table)
        self.assertIs(test_dtr_table['optional_indexed_ref_col'].target.table, test_table)

        test_dtr_table.add_row(ref_col=0, indexed_ref_col=[0, 1],
                               optional_ref_col=0, optional_indexed_ref_col=[0, 1])
        test_dtr_table.add_row(ref_col=0, indexed_ref_col=[0, 1],
                               optional_ref_col=0, optional_indexed_ref_col=[0, 1])

        np.testing.assert_array_equal(test_dtr_table['optional_indexed_ref_col'].target.data, [0, 1, 0, 1])
        np.testing.assert_array_equal(test_dtr_table['optional_ref_col'].data, [0, 0])

    def test_dynamic_table_region_bad_target_col(self):
        test_table = self.TestTable(name='test_table', description='my test table')
        test_table.add_row(my_col=3.0, indexed_col=[1.0, 3.0], optional_col2=.5)
        test_table.add_row(my_col=4.0, indexed_col=[2.0, 4.0], optional_col2=.5)

        msg = r"^'bad' is not the name of a predefined column of table .*"
        with self.assertRaisesRegex(ValueError, msg):
            self.TestDTRTable(name='test_dtr_table', description='my table', target_tables={'bad': test_table})

    def test_dynamic_table_region_non_dtr_target(self):
        test_table = self.TestTable(name='test_table', description='my test table')
        test_table.add_row(my_col=3.0, indexed_col=[1.0, 3.0], optional_col2=.5)
        test_table.add_row(my_col=4.0, indexed_col=[2.0, 4.0], optional_col2=.5)

        msg = "Column 'optional_col3' must be a DynamicTableRegion to have a target table."
        with self.assertRaisesWith(ValueError, msg):
            self.TestDTRTable(name='test_dtr_table', description='my table',
                              target_tables={'optional_col3': test_table})

    def test_roundtrip(self):
        # NOTE this does not use H5RoundTripMixin because this requires custom validation
        test_table = self.TestTable(name='test_table', description='my test table')
        test_table.add_column('dynamic_column', 'this is a dynamic column')
        test_table.add_row(
            my_col=3.0, indexed_col=[1.0, 3.0], dynamic_column=4, optional_col2=.5,
        )
        self.filename = os.path.join(self.test_dir, 'test_TestTable.h5')

        with HDF5IO(self.filename, manager=self.manager, mode='w') as write_io:
            write_io.write(test_table, cache_spec=True)

        self.reader = HDF5IO(self.filename, manager=self.manager, mode='r')
        read_container = self.reader.read()

        self.assertIsNotNone(str(test_table))  # added as a test to make sure printing works
        self.assertIsNotNone(str(read_container))
        # make sure we get a completely new object
        self.assertNotEqual(id(test_table), id(read_container))
        # the name of the root container of a file is always 'root' (see h5tools.py ROOT_NAME)
        # thus, ignore the name of the container when comparing original container vs read container
        self.assertContainerEqual(read_container, test_table, ignore_name=True)

        builder = self.reader.read_builder()
        # TODO fix ValueError: No specification for 'Container' in namespace 'test_core'
        validator = ValidatorMap(self.manager.namespace_catalog.get_namespace(name=CORE_NAMESPACE))
        errors = validator.validate(builder)
        if errors:
            for err in errors:
                raise Exception(err)
        self.reader.close()
