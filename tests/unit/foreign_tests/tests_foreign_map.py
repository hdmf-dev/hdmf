import unittest
from abc import ABCMeta, abstractmethod

import numpy as np

from hdmf import Container, Data
from hdmf.backends.hdf5 import H5DataIO
from hdmf.build import TypeMap, BuildManager, ObjectMapper
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


class Qux(Container):

    @property
    def data_type(self):
        return 'Qux'


class Xyzzy(Data):

    @property
    def data_type(self):
        return 'Xyzzy'


class Bar(Container):

    __fields__ = ('attr1',
                  'dset1',
                  {'name': 'foo1', 'child': True},
                  {'name': 'baz1', 'child': True},
                  {'name': 'some_quxes', 'child': True},
                  {'name': 'some_xyzzies', 'child': True}, )

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Bar'},
            {'name': 'attr1', 'type': (str, ForeignField), 'doc': 'an attribute'},
            {'name': 'dset1', 'type': ('array_data', ForeignField), 'doc': 'a dataset'},
            {'name': 'foo1', 'type': (Foo, ForeignField), 'doc': 'a group data_type', 'default': None},
            {'name': 'baz1', 'type': (Baz, ForeignField), 'doc': 'a dataset data_type', 'default': None},
            {'name': 'some_quxes', 'type': (tuple, list, dict), 'doc': 'multiple group data_types', 'default': None},
            {'name': 'some_xyzzies', 'type': (tuple, list, dict), 'doc': 'multiple dataset data_types', 'default': None})
    def __init__(self, **kwargs):
        name, attr1, dset1, foo1, baz1 = getargs('name', 'attr1', 'dset1', 'foo1', 'baz1', kwargs)
        super().__init__(name=name)
        self.attr1 = attr1
        self.dset1 = dset1
        self.foo1 = foo1
        self.baz1 = baz1
        self.some_quxes = kwargs['some_quxes']
        self.some_xyzzies = kwargs['some_xyzzies']

class BarMap(ObjectMapper):

    def __init__(self, spec):
        super().__init__(spec)
        self.map_spec('some_quxes', spec.get_data_type('Qux'))
        self.map_spec('some_xyzzies', spec.get_data_type('Xyzzy'))

class TestGetSubSpec(TestCase):

    def setUp(self):
        # these are for specific container instances i.e. named in the parent container
        self.foo_spec = GroupSpec('A simple container', data_type_def='Foo')
        self.baz_spec = DatasetSpec('A simple dataset container', data_type_def='Baz',
                                    dtype='float', shape=(None, None))

        # these are for testing multicontainer functionality i.e. one or more in the parent container
        self.qux_spec = GroupSpec('Another simple container', data_type_def='Qux')
        self.xyzzy_spec = DatasetSpec('Another simple dataset container', data_type_def='Xyzzy',
                                      dtype='int', shape=(None, None))

        self.bar_spec = GroupSpec('A container with some stuff', data_type_def='Bar')
        self.bar_spec.add_attribute('attr1', 'a scalar attribute', dtype='text')
        self.bar_spec.add_dataset('a dataset', name='dset1', dtype='int', shape=(None,))
        self.bar_spec.add_group('a sub container', name='foo1', data_type_inc='Foo')
        self.bar_spec.add_dataset('a sub container dataset', name='baz1', data_type_inc='Baz')

        # add the multicontainer data types
        self.bar_spec.add_group('some sub containers', data_type_inc='Qux', quantity='+')
        self.bar_spec.add_dataset('some sub container datasets', data_type_inc='Xyzzy', quantity='+')


        spec_catalog = SpecCatalog()
        spec_catalog.register_spec(self.bar_spec, 'test.yaml')
        spec_catalog.register_spec(self.baz_spec, 'test.yaml')
        spec_catalog.register_spec(self.foo_spec, 'test.yaml')
        spec_catalog.register_spec(self.qux_spec, 'test.yaml')
        spec_catalog.register_spec(self.xyzzy_spec, 'test.yaml')
        namespace = SpecNamespace('a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}],
                                  version='0.1.0',
                                  catalog=spec_catalog)
        namespace_catalog = NamespaceCatalog()
        namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
        self.type_map = TypeMap(namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Bar', Bar)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Foo', Foo)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Baz', Baz)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Qux', Qux)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Xyzzy', Xyzzy)
        self.type_map.register_map(Bar, BarMap)
        self.manager = BuildManager(self.type_map)

    def build_bar(self, **kwargs):
        attr1 = kwargs.get('attr1', 'test attribute')
        dset1 = kwargs.get('dset1', np.array([100, 101, 102], dtype=int))
        foo1 = kwargs.get('foo1', Foo('foo1'))
        some_quxes = kwargs.get('some_quxes', [Qux('test_qux')])
        some_xyzzies = kwargs.get('some_xyzzies', [Xyzzy('test_xyzzy', np.array([[10, 11, 12, 13], [14, 15, 16, 17]], dtype=int))])
        baz1 = kwargs.get('baz1', Baz('test baz', np.array([[0, 1, 2, 3], [4, 5, 6, 7]], dtype=float)))
        bar = Bar('test_bar', attr1, dset1, foo1, baz1, some_quxes=some_quxes, some_xyzzies=some_xyzzies)
        return bar, self.manager.build(bar)

    def check_ff(self, bar, builder, **kwargs):
        self.assertEqual(len(builder.foreign_fields), 1)
        ff = builder.foreign_fields[0]
        self.assertEqual(ff.parent, bar)
        for k, v in kwargs.items():
            with self.subTest(attr=k, val=v):
                self.assertEqual(getattr(ff, k), v)

    def test_attr_ff(self):
        uri = 'http://foobazhub.org/foo/uuid1'
        foo_ff = ForeignField(uri)
        bar, bldr = self.build_bar(attr1=foo_ff)
        self.check_ff(bar, bldr, uri=uri)

    def test_dset_ff(self):
        uri = 'http://foobazhub.org/foo/uuid1'
        foo_ff = ForeignField(uri)
        bar, bldr = self.build_bar(dset1=foo_ff)
        self.check_ff(bar, bldr, uri=uri)

    def test_Container_ff(self):
        uri = 'http://foobazhub.org/foo/uuid1'
        foo_ff = ForeignField(uri)
        bar, bldr = self.build_bar(foo1=foo_ff)
        self.check_ff(bar, bldr, uri=uri, name='foo1')

    def test_Data_ff(self):
        uri = 'http://foobazhub.org/foo/uuid1'
        foo_ff = ForeignField(uri)
        bar, bldr = self.build_bar(baz1=foo_ff)
        self.check_ff(bar, bldr, uri=uri, name='baz1')

    def test_multi_Container_ff_no_spec(self):
        uri = 'http://foobazhub.org/foo/uuid1'
        foo_ff = ForeignField(uri, data_type='Corge', namespace=CORE_NAMESPACE)
        error_msg = ("ambiguous ForeignField found for ambiguous spec -- ForeignField found "
                     "on Bar 'test_bar' in attribute 'some_quxes' must have 'path' set")
        with self.assertRaisesWith(ValueError, error_msg):
            bar, bldr = self.build_bar(some_quxes=[foo_ff])

    def test_multi_Data_ff_no_spec(self):
        uri = 'http://foobazhub.org/foo/uuid1'
        foo_ff = ForeignField(uri, data_type='Corge', namespace=CORE_NAMESPACE)
        error_msg = ("ambiguous ForeignField found for ambiguous spec -- ForeignField found "
                     "on Bar 'test_bar' in attribute 'some_xyzzies' must have 'path' set")
        with self.assertRaisesWith(ValueError, error_msg):
            bar, bldr = self.build_bar(some_xyzzies=[foo_ff])



    def test_multi_Container_ff_no_path(self):
        uri = 'http://foobazhub.org/foo/uuid1'
        foo_ff = ForeignField(uri, data_type='Qux', namespace=CORE_NAMESPACE)
        error_msg = ("ambiguous ForeignField found for ambiguous spec -- ForeignField found "
                     "on Bar 'test_bar' in attribute 'some_quxes' must have 'path' set")
        with self.assertRaisesWith(ValueError, error_msg):
            bar, bldr = self.build_bar(some_quxes=[foo_ff])

    def test_multi_Data_ff_no_path(self):
        uri = 'http://foobazhub.org/foo/uuid1'
        foo_ff = ForeignField(uri, data_type='Xyzzy', namespace=CORE_NAMESPACE)
        error_msg = ("ambiguous ForeignField found for ambiguous spec -- ForeignField found "
                     "on Bar 'test_bar' in attribute 'some_xyzzies' must have 'path' set")
        with self.assertRaisesWith(ValueError, error_msg):
            bar, bldr = self.build_bar(some_xyzzies=[foo_ff])

    def test_multi_Container_ff_no_dt(self):
        uri = 'http://foobazhub.org/foo/uuid1'
        error_msg = "Found 'namespace' but no 'data_type' on ForeignField pointing to http://foobazhub.org/foo/uuid1"
        with self.assertRaisesWith(ValueError, error_msg):
            foo_ff = ForeignField(uri, path='text_qux', namespace=CORE_NAMESPACE)

    def test_multi_Data_ff_no_dt(self):
        uri = 'http://foobazhub.org/foo/uuid1'
        error_msg = "Found 'namespace' but no 'data_type' on ForeignField pointing to http://foobazhub.org/foo/uuid1"
        with self.assertRaisesWith(ValueError, error_msg):
            foo_ff = ForeignField(uri, path='test_xyzzy', namespace=CORE_NAMESPACE)

    def test_multi_Container_ff_no_ns(self):
        uri = 'http://foobazhub.org/foo/uuid1'
        error_msg = "Found 'data_type' but no 'namespace' on ForeignField pointing to http://foobazhub.org/foo/uuid1"
        with self.assertRaisesWith(ValueError, error_msg):
            foo_ff = ForeignField(uri, path='text_qux', data_type='Qux')

    def test_multi_Data_ff_no_ns(self):
        uri = 'http://foobazhub.org/foo/uuid1'
        error_msg = "Found 'data_type' but no 'namespace' on ForeignField pointing to http://foobazhub.org/foo/uuid1"
        with self.assertRaisesWith(ValueError, error_msg):
            foo_ff = ForeignField(uri, path='test_xyzzy', data_type='Xyzzy')


    # Bar - parent container (i.e. group)
    # Foo - group container
    # Baz - dataset container
    # Qux - multi group container
    # Xyzzy - multi dataset container
