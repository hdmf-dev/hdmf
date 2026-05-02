import datetime
import os
import shutil
import tempfile

from hdmf.backends.hdf5 import HDF5IO
from hdmf.build import BuildManager
from hdmf.common import get_type_map
from hdmf.spec import AttributeSpec, DatasetSpec, GroupSpec
from hdmf.testing import TestCase
from tests.unit.helpers.utils import CORE_NAMESPACE, create_load_namespace_yaml


class TestExtensionDatetime(TestCase):
    def setUp(self):
        self.dataset1_spec = DatasetSpec(
            data_type_def='TestDatasetNoDtypeInDef',
            data_type_inc='Data',
            doc='a scalar Dataset without a specified dtype',
        )
        self.dataset2_spec = DatasetSpec(
            data_type_def='TestDatasetWithDtypeInDef',
            data_type_inc='Data',
            doc='a scalar Dataset with dtype=datetime',
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
        self.dataset3_spec = DatasetSpec(
            data_type_def='TestArrayDatasetWithDtypeInDef',
            data_type_inc='Data',
            doc='a 1-D Dataset with dtype=datetime',
            dtype='datetime',
            dims=('num_times',),
            shape=(None,),
        )

        self.group_spec = GroupSpec(
            data_type_def='TestGroup',
            data_type_inc='Container',
            doc='A test group that contains datetime datasets',
            datasets=[
                DatasetSpec(
                    data_type_inc='TestDatasetNoDtypeInDef',
                    name='my_data1',
                    doc='scalar dataset, dtype added at the inc site',
                    dtype='datetime',
                    quantity='?',
                ),
                DatasetSpec(
                    data_type_inc='TestDatasetWithDtypeInDef',
                    name='my_data2',
                    doc='scalar dataset, dtype already on the def',
                    quantity='?',
                ),
                DatasetSpec(
                    name='my_data3',
                    doc='untyped scalar dataset with dtype=datetime',
                    dtype='datetime',
                    quantity='?',
                ),
                DatasetSpec(
                    data_type_inc='TestArrayDatasetWithDtypeInDef',
                    name='my_array_data',
                    doc='1-D datetime dataset',
                    quantity='?',
                ),
                DatasetSpec(
                    name='my_untyped_array',
                    doc='untyped 1-D dataset with dtype=datetime',
                    dtype='datetime',
                    dims=('num_times',),
                    shape=(None,),
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

        self.test_dir = tempfile.mkdtemp()
        self.type_map = get_type_map()
        create_load_namespace_yaml(
            namespace_name=CORE_NAMESPACE,
            specs=[self.dataset1_spec, self.dataset2_spec, self.dataset3_spec, self.group_spec],
            output_dir=self.test_dir,
            incl_types={'hdmf-common': None},
            type_map=self.type_map,
        )
        self.manager = BuildManager(self.type_map)

        self.TestDatasetNoDtypeInDef = self.type_map.get_dt_container_cls('TestDatasetNoDtypeInDef', CORE_NAMESPACE)
        self.TestDatasetWithDtypeInDef = self.type_map.get_dt_container_cls('TestDatasetWithDtypeInDef', CORE_NAMESPACE)
        self.TestArrayDatasetWithDtypeInDef = self.type_map.get_dt_container_cls(
            'TestArrayDatasetWithDtypeInDef', CORE_NAMESPACE
        )
        self.TestGroup = self.type_map.get_dt_container_cls('TestGroup', CORE_NAMESPACE)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir)

    def test_roundtrip_scalar(self):
        group = self.TestGroup(name='root')
        group.my_data1 = self.TestDatasetNoDtypeInDef(name='my_data1', data=datetime.datetime(2020, 1, 1, 0, 0, 0))
        group.my_data2 = self.TestDatasetWithDtypeInDef(
            name='my_data2',
            data=datetime.datetime(2020, 1, 1, 0, 0, 0),
            my_attr=datetime.datetime(2020, 1, 1, 0, 0, 0),
        )
        group.my_data3 = datetime.datetime(2020, 1, 1, 0, 0, 0)
        group.my_attr = datetime.datetime(2020, 1, 1, 0, 0, 0)

        h5_path = os.path.join(self.test_dir, 'test_scalar.h5')
        with HDF5IO(h5_path, 'w', manager=self.manager) as f:
            f.write(group)
        with HDF5IO(h5_path, 'r', manager=self.manager) as f:
            group_read = f.read()
            self.assertContainerEqual(group_read, group, ignore_hdmf_attrs=True)
            self.assertEqual(group_read.my_data1.data, datetime.datetime(2020, 1, 1, 0, 0, 0))
            self.assertEqual(group_read.my_data2.data, datetime.datetime(2020, 1, 1, 0, 0, 0))
            # my_data3 has no data_type_inc, so it is stored as a named scalar dataset on the group
            # and accessed as a plain datetime value rather than a wrapped Data container.
            self.assertEqual(group_read.my_data3, datetime.datetime(2020, 1, 1, 0, 0, 0))
            self.assertEqual(group_read.my_data2.my_attr, datetime.datetime(2020, 1, 1, 0, 0, 0))
            self.assertEqual(group_read.my_attr, datetime.datetime(2020, 1, 1, 0, 0, 0))

    def test_roundtrip_array(self):
        times = [datetime.datetime(2020, 1, 1) + datetime.timedelta(days=i) for i in range(3)]
        group = self.TestGroup(name='root')
        group.my_array_data = self.TestArrayDatasetWithDtypeInDef(name='my_array_data', data=times)
        group.my_untyped_array = times

        h5_path = os.path.join(self.test_dir, 'test_array.h5')
        with HDF5IO(h5_path, 'w', manager=self.manager) as f:
            f.write(group)
        with HDF5IO(h5_path, 'r', manager=self.manager) as f:
            group_read = f.read()
            self.assertContainerEqual(group_read, group, ignore_hdmf_attrs=True)
            self.assertEqual(list(group_read.my_array_data.data), times)
            self.assertEqual(list(group_read.my_untyped_array), times)
