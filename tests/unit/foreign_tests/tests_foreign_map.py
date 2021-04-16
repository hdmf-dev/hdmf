import unittest
from abc import ABCMeta, abstractmethod

import numpy as np

from hdmf import Container, Data
from hdmf.backends.hdf5 import H5DataIO
from hdmf.build import TypeMap, BuildManager
from hdmf.foreign import ForeignField
from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec, SpecCatalog, SpecNamespace, NamespaceCatalog, RefSpec
from hdmf.testing import TestCase
from hdmf.utils import docval, getargs

from tests.unit.utils import CORE_NAMESPACE


class Foo(Container):

    @property
    def data_type(self):
        return 'Foo'


class Baz(Data):

    @property
    def data_type(self):
        return 'Baz'


class Bar(Container):

    __fields__ = ('attr1', 'dset1', 'foo1', 'baz1')

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Bar'},
            {'name': 'attr1', 'type': (str, ForeignField), 'doc': 'an attribute'},
            {'name': 'dset1', 'type': ('array_data', ForeignField), 'doc': 'a dataset'},
            {'name': 'foo1', 'type': (Foo, ForeignField), 'doc': 'a group', 'default': None},
            {'name': 'baz1', 'type': (Baz, ForeignField), 'doc': 'a group', 'default': None})
    def __init__(self, **kwargs):
        name, attr1, dset1, foo1, baz1 = getargs('name', 'attr1', 'dset1', 'foo1', 'baz1', kwargs)
        super().__init__(name=name)
        self.attr1 = attr1
        self.dset1 = dset1
        self.foo1 = foo1
        self.baz1 = baz1

class TestGetSubSpec(TestCase):

    def setUp(self):
        self.bar_spec = GroupSpec('A container with some stuff', data_type_def='Bar')
        self.foo_spec = GroupSpec('A simple container', data_type_def='Foo')
        self.baz_spec = DatasetSpec('A simple dataset container', data_type_def='Baz',
                                    dtype='float', shape=(None, None))

        self.bar_spec.add_attribute('attr1', 'a scalar attribute', dtype='text')
        self.bar_spec.add_dataset('a dataset', name='dset1', dtype='int', shape=(None,))
        self.bar_spec.add_group('a sub container', name='foo1', data_type_inc='Foo')
        self.bar_spec.add_dataset('a sub container dataset', name='baz1', data_type_inc='Baz')

        spec_catalog = SpecCatalog()
        spec_catalog.register_spec(self.bar_spec, 'test.yaml')
        spec_catalog.register_spec(self.baz_spec, 'test.yaml')
        spec_catalog.register_spec(self.foo_spec, 'test.yaml')
        namespace = SpecNamespace('a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}],
                                  version='0.1.0',
                                  catalog=spec_catalog)
        namespace_catalog = NamespaceCatalog()
        namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
        self.type_map = TypeMap(namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Bar', Bar)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Foo', Foo)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Baz', Baz)
        self.manager = BuildManager(self.type_map)

    def build_bar(self, **kwargs):
        attr1 = kwargs.get('attr1', 'test attribute')
        dset1 = kwargs.get('dset1', np.array([100, 101, 102], dtype=int))
        foo1 = kwargs.get('foo1', Foo('test foo'))
        baz1 = kwargs.get('baz1', Baz('test baz', np.array([[0, 1, 2, 3], [4, 5, 6, 7]], dtype=float)))
        bar = Bar('test_bar', attr1, dset1, foo1, baz1)
        self.manager.build(bar)
        return bar

    def test_attr_ff(self):
        foo_ff = ForeignField('http://foobazhub.org/foo/uuid1')
        bar = self.build_bar(attr1=foo_ff)

    def test_dset_ff(self):
        foo_ff = ForeignField('http://foobazhub.org/foo/uuid1')
        bar = self.build_bar(dset1=foo_ff)

    def test_Container_ff(self):
        foo_ff = ForeignField('http://foobazhub.org/foo/uuid1')
        bar = self.build_bar(foo1=foo_ff)

    def test_Data_ff(self):
        foo_ff = ForeignField('http://foobazhub.org/foo/uuid1')
        bar = self.build_bar(baz1=foo_ff)
