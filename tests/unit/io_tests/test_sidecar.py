import json
import numpy as np
import os

from hdmf import Container, Data
from hdmf.backends.hdf5.h5tools import HDF5IO
from hdmf.backends import SidecarValidationError
from hdmf.build import BuildManager, TypeMap, ObjectMapper
from hdmf.spec import AttributeSpec, DatasetSpec, GroupSpec, SpecCatalog, SpecNamespace, NamespaceCatalog
from hdmf.testing import TestCase
from hdmf.utils import getargs, docval


class TestBasic(TestCase):

    def setUp(self):
        self.json_path = "./tests/unit/io_tests/test_sidecar.json"
        self.h5_path = "./tests/unit/io_tests/test_sidecar.h5"
        self.foo_data1 = FooData(name='foodata1', data=[1], data_attr1=2, data_attr2=['a'])
        self.foo2 = Foo(name='foo2', my_data=[1, 2, 3], my_sub_data=[1, 2, 3], attr1='old')
        self.sub_foo = Foo(name='sub_foo', my_data=[-1, -2, -3], my_sub_data=[-1, -2, -3], attr1='OLD')
        self.foo_data = FooData(name='my_foo_data', data=[0, 1], data_attr1=1, data_attr2=['a'])
        self.foo1 = Foo(name='foo1', my_data=[1, 2, 3], my_sub_data=[1, 2, 3], attr1='old', attr2=[17],
                        sub_foo=self.sub_foo, foo_holder_foos=[self.foo2], my_foo_data=self.foo_data,
                        attr3=[1], attr4=[1], attr5=[1])
        with HDF5IO(self.h5_path, manager=_get_manager(), mode='w') as io:
            io.write(self.foo1)

        # operations = [
        #     {
        #         "type": "create_attribute",
        #         "description": "create sub_foo/my_data/attr2 and set it to [1] (int16)",
        #         "object_id": sub_foo.object_id,
        #         "relative_path": "my_data/attr2",
        #         "value": [1],
        #         "dtype": "int16"
        #     },
        #     {
        #         "type": "delete",
        #         "description": "delete foo1/my_data/attr2",
        #         "object_id": foo1.object_id,
        #         "relative_path": "my_data/attr2",
        #     },
        #     {
        #         "type": "change_dtype",
        #         "description": "change dtype of foo1/my_data from int16 to int8",
        #         "object_id": foo1.object_id,
        #         "relative_path": "my_data",
        #         "dtype": "int8"
        #     }
        # ]
        #
        # sidecar = dict()
        # sidecar["operations"] = operations
        # sidecar["schema_version"] = "0.1.0"
        #
        # self.json_path = "./tests/unit/io_tests/test_sidecar.json"
        # with open(self.json_path, 'w') as outfile:
        #     json.dump(sidecar, outfile, indent=4)

    def tearDown(self):
        if os.path.exists(self.h5_path):
            os.remove(self.h5_path)
        if os.path.exists(self.json_path):
            os.remove(self.json_path)

    def _write_test_sidecar(self, operations):
        sidecar = dict()
        sidecar["description"] = "Summary of changes"
        sidecar["author"] = ["The NWB Team"]
        sidecar["contact"] = ["contact@nwb.org"]
        sidecar["operations"] = operations
        sidecar["schema_version"] = "0.1.0"

        with open(self.json_path, 'w') as outfile:
            json.dump(sidecar, outfile, indent=4)

    def test_replace_typed_group_dataset_data(self):
        """Test replacing the data of a dataset in a typed group."""
        operations = [
            {
                "type": "replace",
                "description": "change foo1/my_data data from [1, 2, 3] to [4, 5] (int8)",
                "object_id": self.foo1.object_id,
                "relative_path": "my_data",
                "value": [4, 5],
                "dtype": "int8"
            },
        ]
        self._write_test_sidecar(operations)
        with HDF5IO(self.h5_path, 'r', manager=_get_manager()) as io:
            read_foo1 = io.read()
            # TODO make sure this checks dtype
            np.testing.assert_array_equal(read_foo1.my_data, np.array([4, 5], dtype=np.dtype('int8')))

    def test_replace_subgroup_dataset_data(self):
        """Test replacing the data of a dataset in a subgroup of a typed group."""
        operations = [
            {
                "type": "replace",
                "description": "change foo1/foo_holder_foos/my_sub_data data from [1, 2, 3] to [4, 5] (int8)",
                "object_id": self.foo1.object_id,
                "relative_path": "foo_holder/my_sub_data",
                "value": [4, 5],
                "dtype": "int8"
            },
        ]
        self._write_test_sidecar(operations)
        with HDF5IO(self.h5_path, 'r', manager=_get_manager()) as io:
            read_foo1 = io.read()
            # TODO make sure this checks dtype
            np.testing.assert_array_equal(read_foo1.my_sub_data, np.array([4, 5], dtype=np.dtype('int8')))

    def test_replace_typed_dataset_data(self):
        """Test replacing the data of a typed dataset."""
        operations = [
            {
                "type": "replace",
                "description": "change my_foo_data data from [0, 1] to [2, 3] (int8)",
                "object_id": self.foo_data.object_id,
                "relative_path": "",
                "value": [2, 3],
                "dtype": "int8"
            },
        ]
        self._write_test_sidecar(operations)
        with HDF5IO(self.h5_path, 'r', manager=_get_manager()) as io:
            read_foo1 = io.read()
            # TODO make sure this checks dtype
            np.testing.assert_array_equal(read_foo1.my_foo_data.data, np.array([2, 3], dtype=np.dtype('int8')))

    def test_replace_typed_group_attribute(self):
        """Test replacing the attribute of a typed group."""
        operations = [
            {
                "type": "replace",
                "description": "change foo1/attr1 from 'old' to 'new' (same dtype)",
                "object_id": self.foo1.object_id,
                "relative_path": "attr1",
                "value": "new"
            },
        ]
        self._write_test_sidecar(operations)
        with HDF5IO(self.h5_path, 'r', manager=_get_manager()) as io:
            read_foo1 = io.read()
            assert read_foo1.attr1 == "new"

    def test_replace_subgroup_attribute(self):
        """Test replacing the attribute of an untyped group in a typed group."""
        operations = [
            {
                "type": "replace",
                "description": "change foo1/foo_holder/attr4 from [1] to [2] (int8)",
                "object_id": self.foo1.object_id,
                "relative_path": "foo_holder/attr4",
                "value": [2],
                "dtype": "int8"
            },
        ]
        self._write_test_sidecar(operations)
        with HDF5IO(self.h5_path, 'r', manager=_get_manager()) as io:
            read_foo1 = io.read()
            # TODO make sure this checks dtype
            np.testing.assert_array_equal(read_foo1.attr4, np.array([2], dtype=np.dtype('int8')))

    def test_replace_typed_group_dataset_attribute(self):
        """Test replacing the attribute of an untyped dataset in a typed group."""
        operations = [
            {
                "type": "replace",
                "description": "change foo1/my_data/attr2 from [1] to [2] (int8)",
                "object_id": self.foo1.object_id,
                "relative_path": "my_data/attr2",
                "value": [2],
                "dtype": "int8"
            },
        ]
        self._write_test_sidecar(operations)
        with HDF5IO(self.h5_path, 'r', manager=_get_manager()) as io:
            read_foo1 = io.read()
            # TODO make sure this checks dtype
            np.testing.assert_array_equal(read_foo1.attr2, np.array([2], dtype=np.dtype('int8')))

    def test_replace_subgroup_dataset_attribute(self):
        """Test replacing the attribute of an untyped dataset in an untyped group in a typed group."""
        operations = [
            {
                "type": "replace",
                "description": "change foo1/foo_holder/my_sub_data/attr5 from [1] to [2] (int8)",
                "object_id": self.foo1.object_id,
                "relative_path": "foo_holder/my_sub_data/attr5",
                "value": [2],
                "dtype": "int8"
            },
        ]
        self._write_test_sidecar(operations)
        with HDF5IO(self.h5_path, 'r', manager=_get_manager()) as io:
            read_foo1 = io.read()
            # TODO make sure this checks dtype
            np.testing.assert_array_equal(read_foo1.attr5, np.array([2], dtype=np.dtype('int8')))

    def test_replace_typed_dataset_attribute(self):
        """Test replacing the attribute of a typed dataset."""
        operations = [
            {
                "type": "replace",
                "description": "change foo1/my_foo_data/data_attr1 from 1 to 2 (int8)",
                "object_id": self.foo_data.object_id,
                "relative_path": "data_attr1",
                "value": 2,
                "dtype": "int8"
            },
        ]
        self._write_test_sidecar(operations)
        with HDF5IO(self.h5_path, 'r', manager=_get_manager()) as io:
            read_foo1 = io.read()
            assert read_foo1.my_foo_data.data_attr1 == 2

    def test_delete_typed_group_req_attribute(self):
        pass

    def test_delete_typed_group_opt_attribute(self):
        pass

    def test_delete_subgroup_req_attribute(self):
        pass

    def test_delete_subgroup_opt_attribute(self):
        pass

    def test_delete_typed_group_dataset_req_attribute(self):
        pass

    def test_delete_typed_group_dataset_opt_attribute(self):
        pass

    def test_delete_subgroup_dataset_req_attribute(self):
        pass

    def test_delete_subgroup_dataset_opt_attribute(self):
        pass

    def test_delete_dataset_req_attribute(self):
        pass

    def test_delete_dataset_opt_attribute(self):
        pass

    def test_cannot_delete_group(self):
        pass

    def test_cannot_delete_dataset(self):
        pass


class TestFailValidation(TestCase):

    def setUp(self):
        self.h5_path = "./tests/unit/io_tests/test_sidecar_fail.h5"
        foo1 = Foo(name='foo1', my_data=[1, 2, 3], my_sub_data=[1, 2, 3], attr1='old', attr2=[17])
        with HDF5IO(self.h5_path, manager=_get_manager(), mode='w') as io:
            io.write(foo1)

        sidecar = dict()

        self.json_path = "./tests/unit/io_tests/test_sidecar_fail.json"
        with open(self.json_path, 'w') as outfile:
            json.dump(sidecar, outfile, indent=4)

    def tearDown(self):
        if os.path.exists(self.h5_path):
            os.remove(self.h5_path)
        if os.path.exists(self.json_path):
            os.remove(self.json_path)

    def test_simple(self):
        with HDF5IO(self.h5_path, 'r', manager=_get_manager()) as io:
            with self.assertRaises(SidecarValidationError):
                io.read()


class Foo(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Foo'},
            {'name': 'my_data', 'type': ('array_data', 'data'), 'doc': 'a 1-D or 2-D integer dataset',
             'shape': ((None, ), (None, None))},
            {'name': 'my_sub_data', 'type': ('array_data', 'data'), 'doc': 'a 1-D or 2-D integer dataset',
             'shape': ((None, ), (None, None))},
            {'name': 'attr1', 'type': str, 'doc': 'a string attribute'},
            {'name': 'optional_data', 'type': ('array_data', 'data'), 'doc': 'a 1-D integer dataset', 'default': None},
            {'name': 'attr2', 'type': ('array_data', 'data'), 'doc': 'a 1-D integer attribute', 'shape': (None, ),
             'default': None},
            {'name': 'attr3', 'type': ('array_data', 'data'), 'doc': 'a 1-D integer attribute', 'default': None},
            {'name': 'attr4', 'type': ('array_data', 'data'), 'doc': 'a 1-D integer attribute', 'default': None},
            {'name': 'attr5', 'type': ('array_data', 'data'), 'doc': 'a 1-D integer attribute', 'default': None},
            {'name': 'sub_foo', 'type': 'Foo', 'doc': 'a child Foo', 'default': None},
            {'name': 'my_foo_data', 'type': 'FooData', 'doc': 'a child Foo', 'default': None},
            {'name': 'foo_holder_foos', 'type': ('array_data', 'data'), 'doc': 'child Foos', 'default': None},
            {'name': 'foo_holder_foo_data', 'type': ('array_data', 'data'), 'doc': 'child FooData', 'default': None})
    def __init__(self, **kwargs):
        name, my_data, my_sub_data, attr1, = getargs('name', 'my_data', 'my_sub_data', 'attr1', kwargs)
        optional_data, attr2, attr3, attr4, attr5 = getargs('optional_data', 'attr2', 'attr3', 'attr4', 'attr5', kwargs)
        sub_foo, my_foo_data = getargs('sub_foo', 'my_foo_data', kwargs)
        foo_holder_foos, foo_holder_foo_data = getargs('foo_holder_foos', 'foo_holder_foo_data', kwargs)
        super().__init__(name=name)
        self.my_data = my_data
        self.my_sub_data = my_sub_data
        self.attr1 = attr1
        self.attr2 = attr2
        self.attr3 = attr3
        self.attr4 = attr4
        self.attr5 = attr5
        self.optional_data = optional_data
        self.sub_foo = sub_foo
        if sub_foo is not None:
            assert sub_foo.name == 'sub_foo'  # on read mapping will not work otherwise
            self.sub_foo.parent = self
        self.my_foo_data = my_foo_data
        if my_foo_data is not None:
            assert my_foo_data.name == 'my_foo_data'  # on read mapping will not work otherwise
            self.my_foo_data.parent = self
        self.foo_holder_foos = foo_holder_foos
        if foo_holder_foos is not None:
            for f in foo_holder_foos:
                f.parent = self
        self.foo_holder_foo_data = foo_holder_foo_data
        if foo_holder_foo_data is not None:
            for f in foo_holder_foo_data:
                f.parent = self


class FooData(Data):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Foo'},
            {'name': 'data', 'type': ('array_data', 'data'), 'doc': 'a 1-D or 2-D integer dataset',
             'shape': ((None, ), (None, None))},
            {'name': 'data_attr1', 'type': int, 'doc': 'an integer attribute'},
            {'name': 'data_attr2', 'type': ('array_data', 'data'), 'doc': 'a 1-D text attribute', 'shape': (None, ),
             'default': None})
    def __init__(self, **kwargs):
        name, data, data_attr1, data_attr2 = getargs('name', 'data', 'data_attr1', 'data_attr2', kwargs)
        super().__init__(name=name, data=data)
        self.data_attr1 = data_attr1
        self.data_attr2 = data_attr2


def _get_manager():
    # Foo (group with data_type) has:
    # - groups:
    #   - sub_foo (Foo), 0 or 1
    #   - foo_holder (untyped), required
    #     - groups:
    #       - (Foo), 0 to many, remapped to foo_holder_foos
    #     - datasets:
    #       - my_sub_data (int, 1-D or 2-D), required
    #         - attributes:
    #           - attr5 (int, 1-D), optional, remapped to attr5
    #       - (FooData), 0 to many, remapped to foo_holder_foo_data
    #     - attributes:
    #       - attr4 (int, 1-D), optional, remapped to attr4
    # - datasets:
    #   - my_data (int, 1-D or 2-D), required
    #     - attributes:
    #       - attr2 (int, 1-D), optional, remapped to attr2
    #   - opt_data (int, 1-D or 2-D), 0 or 1, remapped to optional_data
    #     - attributes:
    #       - attr3 (int, 1-D), optional, remapped to attr3
    #   - my_foo_data (FooData), optional
    #  - attributes:
    #    - attr1 (string, scalar), required
    #
    # FooData (dataset with data_type) has:
    # - int, 1D
    # - attributes:
    #   - data_attr1 (int, scalar), required
    #   - data_attr2 (text, 1-D), optional

    foo_data_spec = DatasetSpec(
        doc='A test dataset specification with a data type',
        data_type_def='FooData',
        dtype='int',
        shape=[None, ],
        attributes=[
            AttributeSpec(
                name='data_attr1',
                doc='a scalar integer attribute',
                dtype='int',
            ),
            AttributeSpec(
                name='data_attr2',
                doc='a 1-D text attribute',
                dtype='text',
                shape=[None, ],
                required=False
            )
        ]
    )

    foo_spec = GroupSpec(
        doc='A test group specification with a data type',
        data_type_def='Foo',
        groups=[
            GroupSpec(
                doc='a child Foo',
                data_type_inc='Foo',
                name='sub_foo',
                quantity='?',
            ),
            GroupSpec(
                doc='an untyped group of Foos',
                name='foo_holder',
                quantity='?',
                groups=[
                    GroupSpec(
                        doc='child Foos',
                        data_type_inc='Foo',
                        quantity='*',
                    )
                ],
                datasets=[
                    DatasetSpec(
                        doc='a 1-D or 2-D integer dataset',
                        dtype='int',
                        name='my_sub_data',
                        shape=[[None, ], [None, None]],
                        attributes=[
                            AttributeSpec(
                                name='attr5',
                                doc='a 1-D integer attribute',
                                dtype='int',
                                shape=[None, ],
                                required=False
                            )
                        ]
                    ),
                    DatasetSpec(
                        doc='child FooData',
                        data_type_inc='FooData',
                        quantity='*',
                    )
                ],
                attributes=[
                    AttributeSpec(
                        name='attr4',
                        doc='a 1-D integer attribute',
                        dtype='int',
                        shape=[None, ],
                        required=False
                    )
                ]
            )
        ],
        datasets=[
            DatasetSpec(
                doc='a 1-D or 2-D integer dataset',
                dtype='int',
                name='my_data',
                shape=[[None, ], [None, None]],
                attributes=[
                    AttributeSpec(
                        name='attr2',
                        doc='a 1-D integer attribute',
                        dtype='int',
                        shape=[None, ],
                        required=False
                    )
                ]
            ),
            DatasetSpec(
                doc='an optional 1-D integer dataset',
                dtype='int',
                name='opt_data',
                shape=[None, ],
                quantity='?',
                attributes=[
                    AttributeSpec(
                        name='attr3',
                        doc='a 1-D integer attribute',
                        dtype='int',
                        shape=[None, ],
                        required=False
                    )
                ]
            ),
            DatasetSpec(
                doc='child FooData',
                name='my_foo_data',
                data_type_inc='FooData',
                quantity='?',
            )
        ],
        attributes=[
            AttributeSpec(name='attr1', doc='a string attribute', dtype='text'),
        ]
    )

    class FooMapper(ObjectMapper):
        """Remap spec fields to Container attributes.
        - 'attr2' attribute on Foo container to 'my_data' dataset spec > 'attr2' attribute spec
        - 'attr3' attribute on Foo container to 'opt_data' dataset spec > 'attr3' attribute spec.
        - TODO fill me in
        """
        def __init__(self, spec):
            super().__init__(spec)
            my_data_spec = spec.get_dataset('my_data')
            self.map_spec('attr2', my_data_spec.get_attribute('attr2'))
            opt_data_spec = spec.get_dataset('opt_data')
            self.map_spec('attr3', opt_data_spec.get_attribute('attr3'))
            self.map_spec('optional_data', opt_data_spec)
            foo_holder_spec = spec.get_group('foo_holder')
            self.map_spec('attr4', foo_holder_spec.get_attribute('attr4'))
            self.map_spec('foo_holder_foos', foo_holder_spec.get_data_type('Foo'))
            self.map_spec('foo_holder_foo_data', foo_holder_spec.get_data_type('FooData'))
            my_sub_data_spec = foo_holder_spec.get_dataset('my_sub_data')
            self.map_spec('my_sub_data', my_sub_data_spec)
            self.map_spec('attr5', my_sub_data_spec.get_attribute('attr5'))

    spec_catalog = SpecCatalog()
    spec_catalog.register_spec(foo_spec, 'test.yaml')
    spec_catalog.register_spec(foo_data_spec, 'test.yaml')
    namespace_name = 'test_core'
    namespace = SpecNamespace(
        doc='a test namespace',
        name=namespace_name,
        schema=[{'source': 'test.yaml'}],
        version='0.1.0',
        catalog=spec_catalog
    )
    namespace_catalog = NamespaceCatalog()
    namespace_catalog.add_namespace(namespace_name, namespace)
    type_map = TypeMap(namespace_catalog)
    type_map.register_container_type(namespace_name, 'Foo', Foo)
    type_map.register_container_type(namespace_name, 'FooData', FooData)
    type_map.register_map(Foo, FooMapper)
    manager = BuildManager(type_map)
    return manager
