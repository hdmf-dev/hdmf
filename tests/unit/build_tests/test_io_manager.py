from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec, SpecCatalog, SpecNamespace, NamespaceCatalog
from hdmf.spec.spec import ZERO_OR_MANY
from hdmf.build import GroupBuilder, DatasetBuilder
from hdmf.build import ObjectMapper, BuildManager, TypeMap
from hdmf.testing import TestCase

from abc import ABCMeta, abstractmethod

from tests.unit.utils import Foo, FooBucket, CORE_NAMESPACE


class FooMapper(ObjectMapper):
    """Maps nested 'attr2' attribute on dataset 'my_data' to Foo.attr2 in constructor and attribute map
    """

    def __init__(self, spec):
        super().__init__(spec)
        my_data_spec = spec.get_dataset('my_data')
        self.map_spec('attr2', my_data_spec.get_attribute('attr2'))


class TestBase(TestCase):

    def setUp(self):
        self.foo_spec = GroupSpec('A test group specification with a data type',
                                  data_type_def='Foo',
                                  datasets=[DatasetSpec(
                                      'an example dataset',
                                      'int',
                                      name='my_data',
                                      attributes=[AttributeSpec(
                                          'attr2',
                                          'an example integer attribute',
                                          'int')])],
                                  attributes=[AttributeSpec('attr1', 'an example string attribute', 'text')])

        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.foo_spec, 'test.yaml')
        self.namespace = SpecNamespace(
            'a test namespace',
            CORE_NAMESPACE,
            [{'source': 'test.yaml'}],
            version='0.1.0',
            catalog=self.spec_catalog)
        self.namespace_catalog = NamespaceCatalog()
        self.namespace_catalog.add_namespace(CORE_NAMESPACE, self.namespace)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Foo', Foo)
        self.type_map.register_map(Foo, FooMapper)
        self.manager = BuildManager(self.type_map)


class TestBuildManager(TestBase):

    def get_container_expected_builder(self):
        container_inst = Foo('my_foo', list(range(10)), 'value1', 10)
        expected = GroupBuilder(
            'my_foo',
            datasets={
                'my_data':
                DatasetBuilder(
                    'my_data',
                    list(range(10)),
                    attributes={'attr2': 10})},
            attributes={'attr1': 'value1', 'namespace': CORE_NAMESPACE, 'data_type': 'Foo',
                        'object_id': container_inst.object_id})
        return container_inst, expected

    def test_build(self):
        container_inst, expected = self.get_container_expected_builder()
        builder1 = self.manager.build(container_inst)
        self.assertDictEqual(builder1, expected)
        self.assertIsNone(container_inst.container_source)

    def test_build_set_container_source(self):
        container_inst, expected = self.get_container_expected_builder()
        builder1 = self.manager.build(container_inst, source='file.h5')
        self.assertDictEqual(builder1, expected)
        self.assertEqual(container_inst.container_source, 'file.h5')

    def test_build_set_container_source_preset(self):
        container_inst, expected = self.get_container_expected_builder()
        container_inst.container_source = 'file1.h5'  # as if read from file
        builder1 = self.manager.build(container_inst, source='file1.h5')
        self.assertDictEqual(builder1, expected)
        self.assertEqual(container_inst.container_source, 'file1.h5')

    def test_build_set_container_source_preset_mismatch(self):
        container_inst, expected = self.get_container_expected_builder()
        container_inst.container_source = 'file1.h5'  # as if read from file

        msg = "Cannot change container_source once set: 'my_foo' tests.unit.utils.Foo"
        with self.assertRaisesWith(ValueError, msg):
            self.manager.build(container_inst, source='file2.h5')

    def test_build_memoization(self):
        container_inst = Foo('my_foo', list(range(10)), 'value1', 10)
        expected = GroupBuilder(
            'my_foo',
            datasets={
                'my_data': DatasetBuilder(
                    'my_data',
                    list(range(10)),
                    attributes={'attr2': 10})},
            attributes={'attr1': 'value1', 'namespace': CORE_NAMESPACE, 'data_type': 'Foo',
                        'object_id': container_inst.object_id})
        builder1 = self.manager.build(container_inst)
        builder2 = self.manager.build(container_inst)
        self.assertDictEqual(builder1, expected)
        self.assertIs(builder1, builder2)

    def test_construct(self):
        builder = GroupBuilder(
            'my_foo',
            datasets={
                'my_data': DatasetBuilder(
                    'my_data',
                    list(range(10)),
                    attributes={'attr2': 10})},
            attributes={'attr1': 'value1', 'namespace': CORE_NAMESPACE, 'data_type': 'Foo',
                        'object_id': -1})
        container = self.manager.construct(builder)
        self.assertListEqual(container.my_data, list(range(10)))
        self.assertEqual(container.attr1, 'value1')
        self.assertEqual(container.attr2, 10)

    def test_construct_memoization(self):
        builder = GroupBuilder(
            'my_foo', datasets={'my_data': DatasetBuilder(
                'my_data',
                list(range(10)),
                attributes={'attr2': 10})},
            attributes={'attr1': 'value1', 'namespace': CORE_NAMESPACE, 'data_type': 'Foo',
                        'object_id': -1})
        container1 = self.manager.construct(builder)
        container2 = self.manager.construct(builder)
        self.assertIs(container1, container2)


class NestedBaseMixin(metaclass=ABCMeta):

    def setUp(self):
        super().setUp()
        self.foo_bucket = FooBucket('test_foo_bucket', [
                            Foo('my_foo1', list(range(10)), 'value1', 10),
                            Foo('my_foo2', list(range(10, 20)), 'value2', 20)])
        self.foo_builders = {
            'my_foo1': GroupBuilder('my_foo1',
                                    datasets={'my_data': DatasetBuilder(
                                        'my_data',
                                        list(range(10)),
                                        attributes={'attr2': 10})},
                                    attributes={'attr1': 'value1', 'namespace': CORE_NAMESPACE, 'data_type': 'Foo',
                                                'object_id': self.foo_bucket.foos[0].object_id}),
            'my_foo2': GroupBuilder('my_foo2', datasets={'my_data':
                                                         DatasetBuilder(
                                                             'my_data',
                                                             list(range(10, 20)),
                                                             attributes={'attr2': 20})},
                                    attributes={'attr1': 'value2', 'namespace': CORE_NAMESPACE, 'data_type': 'Foo',
                                                'object_id': self.foo_bucket.foos[1].object_id})
        }
        self.setUpBucketBuilder()
        self.setUpBucketSpec()

        self.spec_catalog.register_spec(self.bucket_spec, 'test.yaml')
        self.type_map.register_container_type(CORE_NAMESPACE, 'FooBucket', FooBucket)
        self.type_map.register_map(FooBucket, self.setUpBucketMapper())
        self.manager = BuildManager(self.type_map)

    @abstractmethod
    def setUpBucketBuilder(self):
        raise NotImplementedError('Cannot run test unless setUpBucketBuilder is implemented')

    @abstractmethod
    def setUpBucketSpec(self):
        raise NotImplementedError('Cannot run test unless setUpBucketSpec is implemented')

    @abstractmethod
    def setUpBucketMapper(self):
        raise NotImplementedError('Cannot run test unless setUpBucketMapper is implemented')

    def test_build(self):
        ''' Test default mapping for an Container that has an Container as an attribute value '''
        builder = self.manager.build(self.foo_bucket)
        self.assertDictEqual(builder, self.bucket_builder)

    def test_construct(self):
        container = self.manager.construct(self.bucket_builder)
        self.assertEqual(container, self.foo_bucket)


class TestNestedContainersNoSubgroups(NestedBaseMixin, TestBase):
    '''
        Test BuildManager.build and BuildManager.construct when the
        Container contains other Containers, but does not keep them in
        additional subgroups
    '''

    def setUpBucketBuilder(self):
        self.bucket_builder = GroupBuilder(
            'test_foo_bucket',
            groups=self.foo_builders,
            attributes={'namespace': CORE_NAMESPACE, 'data_type': 'FooBucket', 'object_id': self.foo_bucket.object_id})

    def setUpBucketSpec(self):
        self.bucket_spec = GroupSpec('A test group specification for a data type containing data type',
                                     name="test_foo_bucket",
                                     data_type_def='FooBucket',
                                     groups=[GroupSpec(
                                         'the Foos in this bucket',
                                         data_type_inc='Foo',
                                         quantity=ZERO_OR_MANY)])

    def setUpBucketMapper(self):
        return ObjectMapper


class TestNestedContainersSubgroup(NestedBaseMixin, TestBase):
    '''
        Test BuildManager.build and BuildManager.construct when the
        Container contains other Containers that are stored in a subgroup
    '''

    def setUpBucketBuilder(self):
        tmp_builder = GroupBuilder('foo_holder', groups=self.foo_builders)
        self.bucket_builder = GroupBuilder(
            'test_foo_bucket',
            groups={'foos': tmp_builder},
            attributes={'namespace': CORE_NAMESPACE, 'data_type': 'FooBucket', 'object_id': self.foo_bucket.object_id})

    def setUpBucketSpec(self):
        tmp_spec = GroupSpec(
            'A subgroup for Foos',
            name='foo_holder',
            groups=[GroupSpec('the Foos in this bucket',
                              data_type_inc='Foo',
                              quantity=ZERO_OR_MANY)])
        self.bucket_spec = GroupSpec('A test group specification for a data type containing data type',
                                     name="test_foo_bucket",
                                     data_type_def='FooBucket',
                                     groups=[tmp_spec])

    def setUpBucketMapper(self):
        class BucketMapper(ObjectMapper):
            def __init__(self, spec):
                super().__init__(spec)
                self.unmap(spec.get_group('foo_holder'))
                self.map_spec('foos', spec.get_group('foo_holder').get_data_type('Foo'))

        return BucketMapper


class TestNestedContainersSubgroupSubgroup(NestedBaseMixin, TestBase):
    '''
        Test BuildManager.build and BuildManager.construct when the
        Container contains other Containers that are stored in a subgroup
        in a subgroup
    '''

    def setUpBucketBuilder(self):
        tmp_builder = GroupBuilder('foo_holder', groups=self.foo_builders)
        tmp_builder = GroupBuilder('foo_holder_holder', groups={'foo_holder': tmp_builder})
        self.bucket_builder = GroupBuilder(
            'test_foo_bucket',
            groups={'foo_holder': tmp_builder},
            attributes={'namespace': CORE_NAMESPACE, 'data_type': 'FooBucket', 'object_id': self.foo_bucket.object_id})

    def setUpBucketSpec(self):
        tmp_spec = GroupSpec('A subgroup for Foos',
                             name='foo_holder',
                             groups=[GroupSpec('the Foos in this bucket',
                                               data_type_inc='Foo',
                                               quantity=ZERO_OR_MANY)])
        tmp_spec = GroupSpec('A subgroup to hold the subgroup', name='foo_holder_holder', groups=[tmp_spec])
        self.bucket_spec = GroupSpec('A test group specification for a data type containing data type',
                                     name="test_foo_bucket",
                                     data_type_def='FooBucket',
                                     groups=[tmp_spec])

    def setUpBucketMapper(self):
        class BucketMapper(ObjectMapper):
            def __init__(self, spec):
                super().__init__(spec)
                self.unmap(spec.get_group('foo_holder_holder'))
                self.unmap(spec.get_group('foo_holder_holder').get_group('foo_holder'))
                self.map_spec('foos', spec.get_group('foo_holder_holder').get_group('foo_holder').get_data_type('Foo'))

        return BucketMapper


class TestTypeMap(TestBase):

    def test_get_ns_dt_missing(self):
        bldr = GroupBuilder('my_foo', attributes={'attr1': 'value1'})
        dt = self.type_map.get_builder_dt(bldr)
        ns = self.type_map.get_builder_ns(bldr)
        self.assertIsNone(dt)
        self.assertIsNone(ns)

    def test_get_ns_dt(self):
        bldr = GroupBuilder('my_foo', attributes={'attr1': 'value1', 'namespace': 'CORE', 'data_type': 'Foo',
                                                  'object_id': -1})
        dt = self.type_map.get_builder_dt(bldr)
        ns = self.type_map.get_builder_ns(bldr)
        self.assertEqual(dt, 'Foo')
        self.assertEqual(ns, 'CORE')


# TODO:
class TestWildCardNamedSpecs(TestCase):
    pass


class TestBuildManagerExport(TestBase):

    def test_constructor(self):
        bm1 = BuildManager(TypeMap())
        self.assertFalse(bm1.export)

        bm1 = BuildManager(TypeMap(), export=True)
        self.assertTrue(bm1.export)

    def get_container_expected_builder(self):
        container_inst = Foo('my_foo', list(range(10)), 'value1', 10)

        dset_builder = DatasetBuilder(
            name='my_data',
            data=list(range(10)),
            attributes={'attr2': 10},
        )
        attrs = {'attr1': 'value1',
                 'namespace': CORE_NAMESPACE,
                 'data_type': 'Foo',
                 'object_id': container_inst.object_id}
        expected = GroupBuilder(
            name='my_foo',
            datasets={'my_data': dset_builder},
            attributes=attrs,
        )
        return container_inst, expected

    def test_build_set_container_source(self):
        container_inst, expected = self.get_container_expected_builder()
        builder1 = self.manager.build(container_inst, source='file1.h5')
        self.assertEqual(container_inst.container_source, 'file1.h5')
        self.assertDictEqual(builder1, expected)

        # building with an export buildmanager does not modify container_source
        bm = BuildManager(self.type_map, export=True)
        builder2 = bm.build(container_inst, source='file2.h5')
        self.assertEqual(container_inst.container_source, 'file1.h5')
        self.assertDictEqual(builder2, expected)
        self.assertFalse(builder1 is builder2)

    def test_build_container_source_preset(self):
        container_inst = Foo('my_foo', list(range(10)), 'value1', 10)
        container_inst.container_source = 'file1.h5'  # as if it were loaded from file

        # building with an export buildmanager does not modify container_source
        bm = BuildManager(self.type_map, export=True)
        bm.build(container_inst, source='file2.h5')
        self.assertEqual(container_inst.container_source, 'file1.h5')

    def test_build_container_source_memoization(self):
        container_inst, expected = self.get_container_expected_builder()
        bm = BuildManager(self.type_map, export=True)
        builder1 = bm.build(container_inst, source='file2.h5')
        builder2 = bm.build(container_inst, source='file2.h5')
        self.assertIs(builder1, builder2)
