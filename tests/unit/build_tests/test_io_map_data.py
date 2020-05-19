from hdmf.spec import AttributeSpec, DatasetSpec, SpecCatalog, SpecNamespace, NamespaceCatalog
from hdmf.build import DatasetBuilder, ObjectMapper, BuildManager, TypeMap
from hdmf import Data
from hdmf.utils import docval, getargs
from hdmf.testing import TestCase

import h5py
import numpy as np
import os

from tests.unit.utils import CORE_NAMESPACE


class Baz(Data):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Baz'},
            {'name': 'data', 'type': (list, h5py.Dataset), 'doc': 'some data'},
            {'name': 'baz_attr', 'type': str, 'doc': 'an attribute'})
    def __init__(self, **kwargs):
        name, data, baz_attr = getargs('name', 'data', 'baz_attr', kwargs)
        super().__init__(name=name, data=data)
        self.__baz_attr = baz_attr

    @property
    def baz_attr(self):
        return self.__baz_attr


class TestDataMap(TestCase):

    def setUp(self):
        self.setUpBazSpec()
        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.baz_spec, 'test.yaml')
        self.namespace = SpecNamespace('a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}],
                                       version='0.1.0',
                                       catalog=self.spec_catalog)
        self.namespace_catalog = NamespaceCatalog()
        self.namespace_catalog.add_namespace(CORE_NAMESPACE, self.namespace)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Baz', Baz)
        self.type_map.register_map(Baz, ObjectMapper)
        self.manager = BuildManager(self.type_map)
        self.mapper = ObjectMapper(self.baz_spec)

    def setUpBazSpec(self):
        self.baz_spec = DatasetSpec('an Baz type', 'int', name='MyBaz', data_type_def='Baz',
                                    attributes=[AttributeSpec('baz_attr', 'an example string attribute', 'text')])

    def test_build(self):
        ''' Test default mapping functionality when no attributes are nested '''
        container = Baz('my_baz', list(range(10)), 'abcdefghijklmnopqrstuvwxyz')
        builder = self.mapper.build(container, self.manager)
        expected = DatasetBuilder('my_baz', list(range(10)), attributes={'baz_attr': 'abcdefghijklmnopqrstuvwxyz'})
        self.assertDictEqual(builder, expected)

    def test_append(self):
        with h5py.File('test.h5', 'w') as file:
            test_ds = file.create_dataset('test_ds', data=[1, 2, 3], chunks=True, maxshape=(None,))
            container = Baz('my_baz', test_ds, 'abcdefghijklmnopqrstuvwxyz')
            container.append(4)
            np.testing.assert_array_equal(container[:], [1, 2, 3, 4])
        os.remove('test.h5')

    def test_extend(self):
        with h5py.File('test.h5', 'w') as file:
            test_ds = file.create_dataset('test_ds', data=[1, 2, 3], chunks=True, maxshape=(None,))
            container = Baz('my_baz', test_ds, 'abcdefghijklmnopqrstuvwxyz')
            container.extend([4, 5])
            np.testing.assert_array_equal(container[:], [1, 2, 3, 4, 5])
        os.remove('test.h5')
