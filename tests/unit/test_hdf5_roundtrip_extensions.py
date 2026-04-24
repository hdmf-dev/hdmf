import datetime
import os
import shutil
import tempfile

from hdmf.backends.hdf5 import HDF5IO
from hdmf.common import get_type_map
from hdmf.testing import TestCase
from hdmf.spec import AttributeSpec,DatasetSpec, GroupSpec, SpecCatalog, SpecNamespace, NamespaceCatalog
from hdmf.build import BuildManager, TypeMap
from tests.unit.helpers.utils import CORE_NAMESPACE


class TestExtensionDatetime(TestCase):
    def setUp(self):

        self.dataset1_spec = DatasetSpec(
            data_type_def='TestDatasetNoDtypeInDef',
            data_type_inc='Data',
            doc='a test Dataset without a specified dtype',  # this is overridden where it is used
        )
        self.dataset2_spec = DatasetSpec(
            data_type_def='TestDatasetWithDtypeInDef',
            data_type_inc='Data',
            doc='a test Dataset with a specified dtype',  # this is overridden where it is used
            dtype='datetime',
            attributes=[
                AttributeSpec(
                    name='my_attr',
                    doc='a scalar datetime attribute',
                    dtype='datetime',
                    required=False,
                )
            ]
        )

        self.group_spec = GroupSpec(
            data_type_def='TestGroup',
            data_type_inc='Container',
            doc='A test group that contains a dataset',
            datasets=[
                # NOTE: these are all scalar datasets
                DatasetSpec(
                    data_type_inc='TestDatasetNoDtypeInDef',
                    name='my_data1',
                    doc='a test Dataset without a specified dtype where the dtype is added in the data_type_inc',
                    dtype='datetime',
                    quantity='?',
                ),
                DatasetSpec(
                    data_type_inc='TestDatasetWithDtypeInDef',
                    name='my_data2',
                    doc='a test Dataset with a specified dtype where the dtype is specified in the data_type_def',
                    quantity='?',
                ),
                DatasetSpec(
                    name='my_data3',
                    doc='a test Dataset with no data_type_inc',
                    dtype='datetime',
                    quantity='?',
                ),
            ],
            attributes=[
                AttributeSpec(
                    name='my_attr',
                    doc='a scalar datetime attribute',
                    dtype='datetime',
                    required=False,
                )
            ]
        )

        from hdmf.spec.write import YAMLSpecWriter
        writer = YAMLSpecWriter(outdir='.')

        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.dataset1_spec, 'test.yaml')
        self.spec_catalog.register_spec(self.dataset2_spec, 'test.yaml')
        self.spec_catalog.register_spec(self.group_spec, 'test.yaml')
        self.namespace = SpecNamespace(
            doc='a test namespace',
            name=CORE_NAMESPACE,
            schema=[
                dict(namespace='hdmf-common'),
                dict(source='test.yaml'),
            ],
            version='0.1.0',
            catalog=self.spec_catalog
        )

        self.test_dir = tempfile.mkdtemp()
        spec_fpath = os.path.join(self.test_dir, 'test.yaml')
        namespace_fpath = os.path.join(self.test_dir, 'test-namespace.yaml')
        writer.write_spec(dict(datasets=[self.dataset1_spec, self.dataset2_spec], groups=[self.group_spec]), spec_fpath)
        writer.write_namespace(self.namespace, namespace_fpath)
        self.namespace_catalog = NamespaceCatalog()
        # We only use Container and Data from hdmf-common
        hdmf_typemap = get_type_map()
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.merge(hdmf_typemap, ns_catalog=True)
        self.type_map.load_namespaces(namespace_fpath)
        self.manager = BuildManager(self.type_map)

        self.TestDatasetNoDtypeInDef = self.type_map.get_dt_container_cls('TestDatasetNoDtypeInDef', CORE_NAMESPACE)
        self.TestDatasetWithDtypeInDef = self.type_map.get_dt_container_cls('TestDatasetWithDtypeInDef', CORE_NAMESPACE)
        self.TestGroup = self.type_map.get_dt_container_cls('TestGroup', CORE_NAMESPACE)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir)

    def test_roundtrip(self):
        group = self.TestGroup(name='my_group')
        group.my_data1 = self.TestDatasetNoDtypeInDef(name='my_data1', data=datetime.datetime(2020, 1, 1, 0, 0, 0))
        group.my_data2 = self.TestDatasetWithDtypeInDef(
            name='my_data2',
            data=datetime.datetime(2020, 1, 1, 0, 0, 0),
            my_attr=datetime.datetime(2020, 1, 1, 0, 0, 0),
        )
        group.my_data3 = datetime.datetime(2020, 1, 1, 0, 0, 0)
        group.my_attr = datetime.datetime(2020, 1, 1, 0, 0, 0)
        with HDF5IO(os.path.join(self.test_dir, 'test.h5'), 'w', manager=self.manager) as f:
            f.write(group)
        with HDF5IO(os.path.join(self.test_dir, 'test.h5'), 'r', manager=self.manager) as f:
            group_read = f.read()
            # HDF5IO names the top-level container 'root' and assigns fresh object_ids on read.
            self.assertContainerEqual(group_read, group, ignore_name=True, ignore_hdmf_attrs=True)
            self.assertEqual(group_read.my_data1.data, datetime.datetime(2020, 1, 1, 0, 0, 0))
            self.assertEqual(group_read.my_data2.data, datetime.datetime(2020, 1, 1, 0, 0, 0))
            # my_data3 has no data_type_inc, so it is stored as a named scalar dataset on the group
            # and accessed as a plain datetime value rather than a wrapped Data container.
            self.assertEqual(group_read.my_data3, datetime.datetime(2020, 1, 1, 0, 0, 0))
            self.assertEqual(group_read.my_data2.my_attr, datetime.datetime(2020, 1, 1, 0, 0, 0))
            self.assertEqual(group_read.my_attr, datetime.datetime(2020, 1, 1, 0, 0, 0))
