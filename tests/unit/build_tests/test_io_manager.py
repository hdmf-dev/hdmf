import unittest2 as unittest

from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec, SpecCatalog, SpecNamespace, NamespaceCatalog
from hdmf.spec.spec import ZERO_OR_MANY
from hdmf.build import GroupBuilder, DatasetBuilder
from hdmf.build import ObjectMapper, BuildManager, TypeMap

from abc import ABCMeta
from six import with_metaclass

from tests.unit.test_utils import Foo, FooBucket, CORE_NAMESPACE


class TestBase(unittest.TestCase):

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
            catalog=self.spec_catalog)
        self.namespace_catalog = NamespaceCatalog()
        self.namespace_catalog.add_namespace(CORE_NAMESPACE, self.namespace)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Foo', Foo)
        self.type_map.register_map(Foo, ObjectMapper)
        self.manager = BuildManager(self.type_map)


class TestBuildManager(TestBase):

    def test_build(self):
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
        builder1 = self.manager.build(container_inst)
        self.assertDictEqual(builder1, expected)

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


class TestNestedBase(with_metaclass(ABCMeta, TestBase)):

    def setUp(self):
        super(TestNestedBase, self).setUp()
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
        self.type_map.register_map(FooBucket, ObjectMapper)
        self.manager = BuildManager(self.type_map)

    def setUpBucketBuilder(self):
        raise unittest.SkipTest('Abstract Base Class')

    def setUpBucketSpec(self):
        raise unittest.SkipTest('Abstract Base Class')

    def test_build(self):
        ''' Test default mapping for an Container that has an Container as an attribute value '''
        builder = self.manager.build(self.foo_bucket)
        self.assertDictEqual(builder, self.bucket_builder)

    def test_construct(self):
        container = self.manager.construct(self.bucket_builder)
        self.assertEqual(container, self.foo_bucket)


class TestNestedContainersNoSubgroups(TestNestedBase):
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


class TestNestedContainersSubgroup(TestNestedBase):
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


class TestNestedContainersSubgroupSubgroup(TestNestedBase):
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
class TestWildCardNamedSpecs(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
