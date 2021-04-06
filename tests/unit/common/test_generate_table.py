import numpy as np
import os
import shutil
import tempfile

from hdmf.build import BuildManager, TypeMap
from hdmf.common import get_type_map, DynamicTable, DynamicTableRegion
from hdmf.spec import GroupSpec, DatasetSpec, SpecCatalog, SpecNamespace, NamespaceCatalog
from hdmf.testing import TestCase

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
                    data_types=[
                         'DynamicTable',
                         'VectorData',
                         'ElementIdentifiers',
                         'DynamicTableRegion',
                         'VectorIndex',
                     ]
                ),
                dict(source='test.yaml'),
                ],
            version='0.1.0',
            catalog=self.spec_catalog)

        self.test_dir = tempfile.mkdtemp()
        spec_fpath = os.path.join(self.test_dir, 'test.yaml')
        namespace_fpath = os.path.join(self.test_dir, 'test-namespace.yaml')
        writer.write_spec(dict(groups=[self.dt_spec, self.dt_spec2]), spec_fpath)
        writer.write_namespace(self.namespace, namespace_fpath)
        self.namespace_catalog = NamespaceCatalog()
        hdmf_typemap = get_type_map()
        self.namespace_catalog.merge(hdmf_typemap.namespace_catalog)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.merge(hdmf_typemap)
        self.type_map.load_namespaces(namespace_fpath)
        self.manager = BuildManager(self.type_map)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir)

    def test_dynamic_table(self):
        TestTable = self.type_map.get_container_cls(CORE_NAMESPACE, 'TestTable')
        TestDTRTable = self.type_map.get_container_cls(CORE_NAMESPACE, 'TestDTRTable')

        assert issubclass(TestTable, DynamicTable)
        print(TestTable)

        assert TestTable.__columns__[0] == dict(
            name='my_col',
            description='a test column'
        )

        test_table = TestTable(name='test_table', description='my test table')

        with self.assertRaises(ValueError):
            test_table.add_row(my_col=3.0, indexed_col=[1.0, 3.0], incorrect_col=5)

        test_table.add_column('dynamic_column', 'this is a dynamic column')

        test_table.add_row(
            my_col=3.0, indexed_col=[1.0, 3.0], dynamic_column=4, optional_col2=.5,
        )
        test_table.add_row(
            my_col=4.0, indexed_col=[2.0, 4.0], dynamic_column=4, optional_col2=.5,
        )

        np.testing.assert_array_equal(test_table['indexed_col'].target.data, [1., 3., 2., 4.])
        np.testing.assert_array_equal(test_table['dynamic_column'].data, [4, 4])

        test_dtr_table = TestDTRTable(name='test_dtr_table', description='my table')
        test_dtr_table.add_row(
            ref_col=DynamicTableRegion(
                name='ref_col',
                description='a test column',
                data=[0],
                table=test_table
            )
        )
