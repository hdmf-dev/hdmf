from hdmf import Container, Data
from hdmf.build import (BuildManager, TypeMap, GroupBuilder, DatasetBuilder, LinkBuilder, ObjectMapper,
                        MissingRequiredBuildWarning, IncorrectQuantityBuildWarning)
from hdmf.spec import GroupSpec, DatasetSpec, LinkSpec, SpecCatalog, SpecNamespace, NamespaceCatalog
from hdmf.spec.spec import ZERO_OR_MANY, ONE_OR_MANY, ZERO_OR_ONE, DEF_QUANTITY
from hdmf.testing import TestCase
from hdmf.utils import docval, getargs

from tests.unit.utils import CORE_NAMESPACE


##########################
# test all crosses:
# {
#   untyped, named group with data-type-included groups / data-type-included datasets / links
#   nested, type definition
#   included groups / included datasets / links
# }
# x
# group/dataset/link with quantity {'*', '+', 1, 2, '?'}
# x
# builder with 2, 1, or 0 instances of the type, or 0 instances of the type with some instances of a mismatched type


class SimpleFoo(Container):
    pass


class NotSimpleFoo(Container):
    pass


class SimpleQux(Data):
    pass


class NotSimpleQux(Data):
    pass


class SimpleBucket(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this SimpleBucket'},
            {'name': 'foos', 'type': list, 'doc': 'the SimpleFoo objects', 'default': list()},
            {'name': 'quxs', 'type': list, 'doc': 'the SimpleQux objects', 'default': list()},
            {'name': 'links', 'type': list, 'doc': 'another way to store SimpleFoo objects', 'default': list()})
    def __init__(self, **kwargs):
        name, foos, quxs, links = getargs('name', 'foos', 'quxs', 'links', kwargs)
        super().__init__(name=name)
        # note: collections of groups are unordered in HDF5, so make these dictionaries for keyed access
        self.foos = {f.name: f for f in foos}
        for f in foos:
            f.parent = self
        self.quxs = {q.name: q for q in quxs}
        for q in quxs:
            q.parent = self
        self.links = {i.name: i for i in links}
        for i in links:
            i.parent = self


class BasicBucket(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this BasicBucket'},
            {'name': 'untyped_dataset', 'type': 'scalar_data',
             'doc': 'a scalar dataset within this BasicBucket', 'default': None},
            {'name': 'untyped_array_dataset', 'type': ('data', 'array_data'),
             'doc': 'an array dataset within this BasicBucket', 'default': None},)
    def __init__(self, **kwargs):
        name, untyped_dataset, untyped_array_dataset = getargs('name', 'untyped_dataset', 'untyped_array_dataset',
                                                               kwargs)
        super().__init__(name=name)
        self.untyped_dataset = untyped_dataset
        self.untyped_array_dataset = untyped_array_dataset


class BuildQuantityMixin:
    """Base test class mixin to set up the BuildManager."""

    def setUpManager(self, specs):
        spec_catalog = SpecCatalog()
        schema_file = 'test.yaml'
        for s in specs:
            spec_catalog.register_spec(s, schema_file)
        namespace = SpecNamespace(
            doc='a test namespace',
            name=CORE_NAMESPACE,
            schema=[{'source': schema_file}],
            version='0.1.0',
            catalog=spec_catalog
        )
        namespace_catalog = NamespaceCatalog()
        namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
        type_map = TypeMap(namespace_catalog)
        type_map.register_container_type(CORE_NAMESPACE, 'SimpleFoo', SimpleFoo)
        type_map.register_container_type(CORE_NAMESPACE, 'NotSimpleFoo', NotSimpleFoo)
        type_map.register_container_type(CORE_NAMESPACE, 'SimpleQux', SimpleQux)
        type_map.register_container_type(CORE_NAMESPACE, 'NotSimpleQux', NotSimpleQux)
        type_map.register_container_type(CORE_NAMESPACE, 'SimpleBucket', SimpleBucket)
        type_map.register_map(SimpleBucket, self.setUpBucketMapper())
        self.manager = BuildManager(type_map)

    def _create_builder(self, container):
        """Helper function to get a basic builder for a container with no subgroups/datasets/links."""
        if isinstance(container, Container):
            ret = GroupBuilder(
                name=container.name,
                attributes={'namespace': container.namespace,
                            'data_type': container.data_type,
                            'object_id': container.object_id}
            )
        else:
            ret = DatasetBuilder(
                name=container.name,
                data=container.data,
                attributes={'namespace': container.namespace,
                            'data_type': container.data_type,
                            'object_id': container.object_id}
            )
        return ret


class TypeIncUntypedGroupMixin:

    def create_specs(self, quantity):
        # Type SimpleBucket contains:
        # - an untyped group "foo_holder" which contains [quantity] groups of data_type_inc SimpleFoo
        # - an untyped group "qux_holder" which contains [quantity] datasets of data_type_inc SimpleQux
        # - an untyped group "link_holder" which contains [quantity] links of target_type SimpleFoo
        foo_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='SimpleFoo',
        )
        not_foo_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='NotSimpleFoo',
        )
        qux_spec = DatasetSpec(
            doc='A test group specification with a data type',
            data_type_def='SimpleQux',
        )
        not_qux_spec = DatasetSpec(
            doc='A test group specification with a data type',
            data_type_def='NotSimpleQux',
        )
        foo_inc_spec = GroupSpec(
            doc='the SimpleFoos in this bucket',
            data_type_inc='SimpleFoo',
            quantity=quantity
        )
        foo_holder_spec = GroupSpec(
            doc='An untyped subgroup for SimpleFoos',
            name='foo_holder',
            groups=[foo_inc_spec]
        )
        qux_inc_spec = DatasetSpec(
            doc='the SimpleQuxs in this bucket',
            data_type_inc='SimpleQux',
            quantity=quantity
        )
        qux_holder_spec = GroupSpec(
            doc='An untyped subgroup for SimpleQuxs',
            name='qux_holder',
            datasets=[qux_inc_spec]
        )
        foo_link_spec = LinkSpec(
            doc='the links in this bucket',
            target_type='SimpleFoo',
            quantity=quantity
        )
        link_holder_spec = GroupSpec(
            doc='An untyped subgroup for links',
            name='link_holder',
            links=[foo_link_spec]
        )
        bucket_spec = GroupSpec(
            doc='A test group specification for a data type containing data type',
            name="test_bucket",
            data_type_def='SimpleBucket',
            groups=[foo_holder_spec, qux_holder_spec, link_holder_spec]
        )
        return [foo_spec, not_foo_spec, qux_spec, not_qux_spec, bucket_spec]

    def setUpBucketMapper(self):
        class BucketMapper(ObjectMapper):
            def __init__(self, spec):
                super().__init__(spec)
                self.unmap(spec.get_group('foo_holder'))
                self.map_spec('foos', spec.get_group('foo_holder').get_data_type('SimpleFoo'))
                self.unmap(spec.get_group('qux_holder'))
                self.map_spec('quxs', spec.get_group('qux_holder').get_data_type('SimpleQux'))
                self.unmap(spec.get_group('link_holder'))
                self.map_spec('links', spec.get_group('link_holder').links[0])

        return BucketMapper

    def get_two_bucket_test(self):
        foos = [SimpleFoo('my_foo1'), SimpleFoo('my_foo2')]
        quxs = [SimpleQux('my_qux1', data=[1, 2, 3]), SimpleQux('my_qux2', data=[4, 5, 6])]
        bucket = SimpleBucket(
            name='test_bucket',
            foos=foos,
            quxs=quxs,
            links=foos
        )
        foo1_builder = self._create_builder(bucket.foos['my_foo1'])
        foo2_builder = self._create_builder(bucket.foos['my_foo2'])
        qux1_builder = self._create_builder(bucket.quxs['my_qux1'])
        qux2_builder = self._create_builder(bucket.quxs['my_qux2'])
        foo_holder_builder = GroupBuilder(
            name='foo_holder',
            groups={'my_foo1': foo1_builder,
                    'my_foo2': foo2_builder}
        )
        qux_holder_builder = GroupBuilder(
            name='qux_holder',
            datasets={'my_qux1': qux1_builder,
                      'my_qux2': qux2_builder}
        )
        foo1_link_builder = LinkBuilder(builder=foo1_builder)
        foo2_link_builder = LinkBuilder(builder=foo2_builder)
        link_holder_builder = GroupBuilder(
            name='link_holder',
            links={'my_foo1': foo1_link_builder,
                   'my_foo2': foo2_link_builder}
        )
        bucket_builder = GroupBuilder(
            name='test_bucket',
            groups={'foos': foo_holder_builder,
                    'quxs': qux_holder_builder,
                    'links': link_holder_builder},
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'SimpleBucket',
                        'object_id': bucket.object_id}
        )
        return bucket, bucket_builder

    def get_one_bucket_test(self):
        foos = [SimpleFoo('my_foo1')]
        quxs = [SimpleQux('my_qux1', data=[1, 2, 3])]
        bucket = SimpleBucket(
            name='test_bucket',
            foos=foos,
            quxs=quxs,
            links=foos
        )
        foo1_builder = GroupBuilder(
            name='my_foo1',
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'SimpleFoo',
                        'object_id': bucket.foos['my_foo1'].object_id}
        )
        foo_holder_builder = GroupBuilder(
            name='foo_holder',
            groups={'my_foo1': foo1_builder}
        )
        qux1_builder = DatasetBuilder(
            name='my_qux1',
            data=[1, 2, 3],
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'SimpleQux',
                        'object_id': bucket.quxs['my_qux1'].object_id}
        )
        qux_holder_builder = GroupBuilder(
            name='qux_holder',
            datasets={'my_qux1': qux1_builder}
        )
        foo1_link_builder = LinkBuilder(builder=foo1_builder)
        link_holder_builder = GroupBuilder(
            name='link_holder',
            links={'my_foo1': foo1_link_builder}
        )
        bucket_builder = GroupBuilder(
            name='test_bucket',
            groups={'foos': foo_holder_builder,
                    'quxs': qux_holder_builder,
                    'links': link_holder_builder},
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'SimpleBucket',
                        'object_id': bucket.object_id}
        )
        return bucket, bucket_builder

    def get_zero_bucket_test(self):
        bucket = SimpleBucket(
            name='test_bucket'
        )
        foo_holder_builder = GroupBuilder(
            name='foo_holder',
            groups={}
        )
        qux_holder_builder = GroupBuilder(
            name='qux_holder',
            datasets={}
        )
        link_holder_builder = GroupBuilder(
            name='link_holder',
            links={}
        )
        bucket_builder = GroupBuilder(
            name='test_bucket',
            groups={'foos': foo_holder_builder,
                    'quxs': qux_holder_builder,
                    'links': link_holder_builder},
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'SimpleBucket',
                        'object_id': bucket.object_id}
        )
        return bucket, bucket_builder

    def get_mismatch_bucket_test(self):
        foos = [NotSimpleFoo('my_foo1'), NotSimpleFoo('my_foo2')]
        quxs = [NotSimpleQux('my_qux1', data=[1, 2, 3]), NotSimpleQux('my_qux2', data=[4, 5, 6])]
        bucket = SimpleBucket(
            name='test_bucket',
            foos=foos,
            quxs=quxs,
            links=foos
        )
        foo_holder_builder = GroupBuilder(
            name='foo_holder',
            groups={}
        )
        qux_holder_builder = GroupBuilder(
            name='qux_holder',
            datasets={}
        )
        link_holder_builder = GroupBuilder(
            name='link_holder',
            links={}
        )
        bucket_builder = GroupBuilder(
            name='test_bucket',
            groups={'foos': foo_holder_builder,
                    'quxs': qux_holder_builder,
                    'links': link_holder_builder},
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'SimpleBucket',
                        'object_id': bucket.object_id}
        )
        return bucket, bucket_builder


class TypeDefMixin:

    def create_specs(self, quantity):
        # Type SimpleBucket contains:
        # - contains [quantity] groups of data_type_def SimpleFoo
        # - contains [quantity] datasets of data_type_def SimpleQux
        # NOTE: links do not have data_type_def, so leave them out of these tests
        # NOTE: nested type definitions are strongly discouraged now
        foo_spec = GroupSpec(
            doc='the SimpleFoos in this bucket',
            data_type_def='SimpleFoo',
            quantity=quantity
        )
        qux_spec = DatasetSpec(
            doc='the SimpleQuxs in this bucket',
            data_type_def='SimpleQux',
            quantity=quantity
        )
        not_foo_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='NotSimpleFoo',
        )
        not_qux_spec = DatasetSpec(
            doc='A test group specification with a data type',
            data_type_def='NotSimpleQux',
        )
        bucket_spec = GroupSpec(
            doc='A test group specification for a data type containing data type',
            name="test_bucket",
            data_type_def='SimpleBucket',
            groups=[foo_spec],
            datasets=[qux_spec]
        )
        return [foo_spec, not_foo_spec, qux_spec, not_qux_spec, bucket_spec]

    def setUpBucketMapper(self):
        class BucketMapper(ObjectMapper):
            def __init__(self, spec):
                super().__init__(spec)
                self.map_spec('foos', spec.get_data_type('SimpleFoo'))
                self.map_spec('quxs', spec.get_data_type('SimpleQux'))

        return BucketMapper

    def get_two_bucket_test(self):
        foos = [SimpleFoo('my_foo1'), SimpleFoo('my_foo2')]
        quxs = [SimpleQux('my_qux1', data=[1, 2, 3]), SimpleQux('my_qux2', data=[4, 5, 6])]
        bucket = SimpleBucket(
            name='test_bucket',
            foos=foos,
            quxs=quxs,
        )
        foo1_builder = self._create_builder(bucket.foos['my_foo1'])
        foo2_builder = self._create_builder(bucket.foos['my_foo2'])
        qux1_builder = self._create_builder(bucket.quxs['my_qux1'])
        qux2_builder = self._create_builder(bucket.quxs['my_qux2'])
        bucket_builder = GroupBuilder(
            name='test_bucket',
            groups={'my_foo1': foo1_builder,
                    'my_foo2': foo2_builder},
            datasets={'my_qux1': qux1_builder,
                      'my_qux2': qux2_builder},
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'SimpleBucket',
                        'object_id': bucket.object_id}
        )
        return bucket, bucket_builder

    def get_one_bucket_test(self):
        foos = [SimpleFoo('my_foo1')]
        quxs = [SimpleQux('my_qux1', data=[1, 2, 3])]
        bucket = SimpleBucket(
            name='test_bucket',
            foos=foos,
            quxs=quxs,
        )
        foo1_builder = self._create_builder(bucket.foos['my_foo1'])
        qux1_builder = self._create_builder(bucket.quxs['my_qux1'])
        bucket_builder = GroupBuilder(
            name='test_bucket',
            groups={'my_foo1': foo1_builder},
            datasets={'my_qux1': qux1_builder},
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'SimpleBucket',
                        'object_id': bucket.object_id}
        )
        return bucket, bucket_builder

    def get_zero_bucket_test(self):
        bucket = SimpleBucket(
            name='test_bucket'
        )
        bucket_builder = GroupBuilder(
            name='test_bucket',
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'SimpleBucket',
                        'object_id': bucket.object_id}
        )
        return bucket, bucket_builder

    def get_mismatch_bucket_test(self):
        foos = [NotSimpleFoo('my_foo1'), NotSimpleFoo('my_foo2')]
        quxs = [NotSimpleQux('my_qux1', data=[1, 2, 3]), NotSimpleQux('my_qux2', data=[4, 5, 6])]
        bucket = SimpleBucket(
            name='test_bucket',
            foos=foos,
            quxs=quxs,
        )
        bucket_builder = GroupBuilder(
            name='test_bucket',
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'SimpleBucket',
                        'object_id': bucket.object_id}
        )
        return bucket, bucket_builder


class TypeIncMixin:

    def create_specs(self, quantity):
        # Type SimpleBucket contains:
        # - [quantity] groups of data_type_inc SimpleFoo
        # - [quantity] datasets of data_type_inc SimpleQux
        # - [quantity] links of target_type SimpleFoo
        foo_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='SimpleFoo',
        )
        not_foo_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='NotSimpleFoo',
        )
        qux_spec = DatasetSpec(
            doc='A test group specification with a data type',
            data_type_def='SimpleQux',
        )
        not_qux_spec = DatasetSpec(
            doc='A test group specification with a data type',
            data_type_def='NotSimpleQux',
        )
        foo_inc_spec = GroupSpec(
            doc='the SimpleFoos in this bucket',
            data_type_inc='SimpleFoo',
            quantity=quantity
        )
        qux_inc_spec = DatasetSpec(
            doc='the SimpleQuxs in this bucket',
            data_type_inc='SimpleQux',
            quantity=quantity
        )
        foo_link_spec = LinkSpec(
            doc='the links in this bucket',
            target_type='SimpleFoo',
            quantity=quantity
        )
        bucket_spec = GroupSpec(
            doc='A test group specification for a data type containing data type',
            name="test_bucket",
            data_type_def='SimpleBucket',
            groups=[foo_inc_spec],
            datasets=[qux_inc_spec],
            links=[foo_link_spec]
        )
        return [foo_spec, not_foo_spec, qux_spec, not_qux_spec, bucket_spec]

    def setUpBucketMapper(self):
        class BucketMapper(ObjectMapper):
            def __init__(self, spec):
                super().__init__(spec)
                self.map_spec('foos', spec.get_data_type('SimpleFoo'))
                self.map_spec('quxs', spec.get_data_type('SimpleQux'))
                self.map_spec('links', spec.links[0])

        return BucketMapper

    def get_two_bucket_test(self):
        foos = [SimpleFoo('my_foo1'), SimpleFoo('my_foo2')]
        quxs = [SimpleQux('my_qux1', data=[1, 2, 3]), SimpleQux('my_qux2', data=[4, 5, 6])]
        # NOTE: unlike in the other tests, links cannot map to the same foos in bucket because of a name clash
        links = [SimpleFoo('my_foo3'), SimpleFoo('my_foo4')]
        bucket = SimpleBucket(
            name='test_bucket',
            foos=foos,
            quxs=quxs,
            links=links
        )
        foo1_builder = self._create_builder(bucket.foos['my_foo1'])
        foo2_builder = self._create_builder(bucket.foos['my_foo2'])
        foo3_builder = self._create_builder(bucket.links['my_foo3'])
        foo4_builder = self._create_builder(bucket.links['my_foo4'])
        qux1_builder = self._create_builder(bucket.quxs['my_qux1'])
        qux2_builder = self._create_builder(bucket.quxs['my_qux2'])
        foo3_link_builder = LinkBuilder(builder=foo3_builder)
        foo4_link_builder = LinkBuilder(builder=foo4_builder)
        bucket_builder = GroupBuilder(
            name='test_bucket',
            groups={'my_foo1': foo1_builder,
                    'my_foo2': foo2_builder},
            datasets={'my_qux1': qux1_builder,
                      'my_qux2': qux2_builder},
            links={'my_foo3': foo3_link_builder,
                   'my_foo4': foo4_link_builder},
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'SimpleBucket',
                        'object_id': bucket.object_id}
        )
        return bucket, bucket_builder

    def get_one_bucket_test(self):
        foos = [SimpleFoo('my_foo1')]
        quxs = [SimpleQux('my_qux1', data=[1, 2, 3])]
        # NOTE: unlike in the other tests, links cannot map to the same foos in bucket because of a name clash
        links = [SimpleFoo('my_foo3')]
        bucket = SimpleBucket(
            name='test_bucket',
            foos=foos,
            quxs=quxs,
            links=links
        )
        foo1_builder = self._create_builder(bucket.foos['my_foo1'])
        foo3_builder = self._create_builder(bucket.links['my_foo3'])
        qux1_builder = self._create_builder(bucket.quxs['my_qux1'])
        foo3_link_builder = LinkBuilder(builder=foo3_builder)
        bucket_builder = GroupBuilder(
            name='test_bucket',
            groups={'my_foo1': foo1_builder},
            datasets={'my_qux1': qux1_builder},
            links={'my_foo1': foo3_link_builder},
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'SimpleBucket',
                        'object_id': bucket.object_id}
        )
        return bucket, bucket_builder

    def get_zero_bucket_test(self):
        bucket = SimpleBucket(
            name='test_bucket'
        )
        bucket_builder = GroupBuilder(
            name='test_bucket',
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'SimpleBucket',
                        'object_id': bucket.object_id}
        )
        return bucket, bucket_builder

    def get_mismatch_bucket_test(self):
        foos = [NotSimpleFoo('my_foo1'), NotSimpleFoo('my_foo2')]
        quxs = [NotSimpleQux('my_qux1', data=[1, 2, 3]), NotSimpleQux('my_qux2', data=[4, 5, 6])]
        links = [NotSimpleFoo('my_foo1'), NotSimpleFoo('my_foo2')]
        bucket = SimpleBucket(
            name='test_bucket',
            foos=foos,
            quxs=quxs,
            links=links
        )
        bucket_builder = GroupBuilder(
            name='test_bucket',
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'SimpleBucket',
                        'object_id': bucket.object_id}
        )
        return bucket, bucket_builder


class ZeroOrManyMixin:

    def setUp(self):
        specs = self.create_specs(ZERO_OR_MANY)
        self.setUpManager(specs)

    def test_build_two(self):
        """Test building a container which contains multiple containers as the spec allows."""
        bucket, bucket_builder = self.get_two_bucket_test()
        builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)

    def test_build_one(self):
        """Test building a container which contains one container as the spec allows."""
        bucket, bucket_builder = self.get_one_bucket_test()
        builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)

    def test_build_zero(self):
        """Test building a container which contains no containers as the spec allows."""
        bucket, bucket_builder = self.get_zero_bucket_test()
        builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)

    def test_build_mismatch(self):
        """Test building a container which contains no containers that match the spec as the spec allows."""
        bucket, bucket_builder = self.get_mismatch_bucket_test()
        builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)


class OneOrManyMixin:

    def setUp(self):
        specs = self.create_specs(ONE_OR_MANY)
        self.setUpManager(specs)

    def test_build_two(self):
        """Test building a container which contains multiple containers as the spec allows."""
        bucket, bucket_builder = self.get_two_bucket_test()
        builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)

    def test_build_one(self):
        """Test building a container which contains one container as the spec allows."""
        bucket, bucket_builder = self.get_one_bucket_test()
        builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)

    def test_build_zero(self):
        """Test building a container which contains no containers as the spec allows."""
        bucket, bucket_builder = self.get_zero_bucket_test()
        msg = r"SimpleBucket 'test_bucket' is missing required value for attribute '.*'\."
        with self.assertWarnsRegex(MissingRequiredBuildWarning, msg):
            builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)

    def test_build_mismatch(self):
        """Test building a container which contains no containers that match the spec as the spec allows."""
        bucket, bucket_builder = self.get_mismatch_bucket_test()
        msg = r"SimpleBucket 'test_bucket' is missing required value for attribute '.*'\."
        with self.assertWarnsRegex(MissingRequiredBuildWarning, msg):
            builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)


class OneMixin:

    def setUp(self):
        specs = self.create_specs(DEF_QUANTITY)
        self.setUpManager(specs)

    def test_build_two(self):
        """Test building a container which contains multiple containers as the spec allows."""
        bucket, bucket_builder = self.get_two_bucket_test()
        msg = r"SimpleBucket 'test_bucket' has 2 values for attribute '.*' but spec allows 1\."
        with self.assertWarnsRegex(IncorrectQuantityBuildWarning, msg):
            builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)

    def test_build_one(self):
        """Test building a container which contains one container as the spec allows."""
        bucket, bucket_builder = self.get_one_bucket_test()
        builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)

    def test_build_zero(self):
        """Test building a container which contains no containers as the spec allows."""
        bucket, bucket_builder = self.get_zero_bucket_test()
        msg = r"SimpleBucket 'test_bucket' is missing required value for attribute '.*'\."
        with self.assertWarnsRegex(MissingRequiredBuildWarning, msg):
            builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)

    def test_build_mismatch(self):
        """Test building a container which contains no containers that match the spec as the spec allows."""
        bucket, bucket_builder = self.get_mismatch_bucket_test()
        msg = r"SimpleBucket 'test_bucket' is missing required value for attribute '.*'\."
        with self.assertWarnsRegex(MissingRequiredBuildWarning, msg):
            builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)


class TwoMixin:

    def setUp(self):
        specs = self.create_specs(2)
        self.setUpManager(specs)

    def test_build_two(self):
        """Test building a container which contains multiple containers as the spec allows."""
        bucket, bucket_builder = self.get_two_bucket_test()
        builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)

    def test_build_one(self):
        """Test building a container which contains one container as the spec allows."""
        bucket, bucket_builder = self.get_one_bucket_test()
        msg = r"SimpleBucket 'test_bucket' has 1 values for attribute '.*' but spec allows 2\."
        with self.assertWarnsRegex(IncorrectQuantityBuildWarning, msg):
            builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)

    def test_build_zero(self):
        """Test building a container which contains no containers as the spec allows."""
        bucket, bucket_builder = self.get_zero_bucket_test()
        msg = r"SimpleBucket 'test_bucket' is missing required value for attribute '.*'\."
        with self.assertWarnsRegex(MissingRequiredBuildWarning, msg):
            builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)

    def test_build_mismatch(self):
        """Test building a container which contains no containers that match the spec as the spec allows."""
        bucket, bucket_builder = self.get_mismatch_bucket_test()
        msg = r"SimpleBucket 'test_bucket' is missing required value for attribute '.*'\."
        with self.assertWarnsRegex(MissingRequiredBuildWarning, msg):
            builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)


class ZeroOrOneMixin:

    def setUp(self):
        specs = self.create_specs(ZERO_OR_ONE)
        self.setUpManager(specs)

    def test_build_two(self):
        """Test building a container which contains multiple containers as the spec allows."""
        bucket, bucket_builder = self.get_two_bucket_test()
        msg = r"SimpleBucket 'test_bucket' has 2 values for attribute '.*' but spec allows '\?'\."
        with self.assertWarnsRegex(IncorrectQuantityBuildWarning, msg):
            builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)

    def test_build_one(self):
        """Test building a container which contains one container as the spec allows."""
        bucket, bucket_builder = self.get_one_bucket_test()
        builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)

    def test_build_zero(self):
        """Test building a container which contains no containers as the spec allows."""
        bucket, bucket_builder = self.get_zero_bucket_test()
        builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)

    def test_build_mismatch(self):
        """Test building a container which contains no containers that match the spec as the spec allows."""
        bucket, bucket_builder = self.get_mismatch_bucket_test()
        builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)


# Untyped group with included groups / included datasets / links with quantity {'*', '+', 1, 2 '?'}

class TestBuildZeroOrManyTypeIncUntypedGroup(ZeroOrManyMixin, TypeIncUntypedGroupMixin, BuildQuantityMixin, TestCase):
    """Test building a group that has an untyped subgroup with a data type inc subgroup/dataset/link with quantity '*'
    """
    pass


class TestBuildOneOrManyTypeIncUntypedGroup(OneOrManyMixin, TypeIncUntypedGroupMixin, BuildQuantityMixin, TestCase):
    """Test building a group that has an untyped subgroup with a data type inc subgroup/dataset/link with quantity '+'
    """
    pass


class TestBuildOneTypeIncUntypedGroup(OneMixin, TypeIncUntypedGroupMixin, BuildQuantityMixin, TestCase):
    """Test building a group that has an untyped subgroup with a data type inc subgroup/dataset/link with quantity 1
    """
    pass


class TestBuildTwoTypeIncUntypedGroup(TwoMixin, TypeIncUntypedGroupMixin, BuildQuantityMixin, TestCase):
    """Test building a group that has an untyped subgroup with a data type inc subgroup/dataset/link with quantity 2
    """
    pass


class TestBuildZeroOrOneTypeIncUntypedGroup(ZeroOrOneMixin, TypeIncUntypedGroupMixin, BuildQuantityMixin, TestCase):
    """Test building a group that has an untyped subgroup with a data type inc subgroup/dataset/link with quantity '?'
    """
    pass


# Nested type definition of group/dataset with quantity {'*', '+', 1, 2, '?'}

class TestBuildZeroOrManyTypeDef(ZeroOrManyMixin, TypeDefMixin, BuildQuantityMixin, TestCase):
    """Test building a group that has a nested type def with quantity '*'
    """
    pass


class TestBuildOneOrManyTypeDef(OneOrManyMixin, TypeDefMixin, BuildQuantityMixin, TestCase):
    """Test building a group that has a nested type def with quantity '+'
    """
    pass


class TestBuildOneTypeDef(OneMixin, TypeDefMixin, BuildQuantityMixin, TestCase):
    """Test building a group that has a nested type def with quantity 1
    """
    pass


class TestBuildTwoTypeDef(TwoMixin, TypeDefMixin, BuildQuantityMixin, TestCase):
    """Test building a group that has a nested type def with quantity 2
    """
    pass


class TestBuildZeroOrOneTypeDef(ZeroOrOneMixin, TypeDefMixin, BuildQuantityMixin, TestCase):
    """Test building a group that has a nested type def with quantity '?'
    """
    pass


# Included groups / included datasets / links with quantity {'*', '+', 1, 2, '?'}

class TestBuildZeroOrManyTypeInc(ZeroOrManyMixin, TypeIncMixin, BuildQuantityMixin, TestCase):
    """Test building a group that has a data type inc subgroup/dataset/link with quantity '*'
    """
    pass


class TestBuildOneOrManyTypeInc(OneOrManyMixin, TypeIncMixin, BuildQuantityMixin, TestCase):
    """Test building a group that has a data type inc subgroup/dataset/link with quantity '+'
    """
    pass


class TestBuildOneTypeInc(OneMixin, TypeIncMixin, BuildQuantityMixin, TestCase):
    """Test building a group that has a data type inc subgroup/dataset/link with quantity 1
    """
    pass


class TestBuildTwoTypeInc(TwoMixin, TypeIncMixin, BuildQuantityMixin, TestCase):
    """Test building a group that has a data type inc subgroup/dataset/link with quantity 2
    """
    pass


class TestBuildZeroOrOneTypeInc(ZeroOrOneMixin, TypeIncMixin, BuildQuantityMixin, TestCase):
    """Test building a group that has a data type inc subgroup/dataset/link with quantity '?'
    """
    pass


# Untyped group/dataset with quantity {1, '?'}

class UntypedMixin:

    def setUpManager(self, specs):
        spec_catalog = SpecCatalog()
        schema_file = 'test.yaml'
        for s in specs:
            spec_catalog.register_spec(s, schema_file)
        namespace = SpecNamespace(
            doc='a test namespace',
            name=CORE_NAMESPACE,
            schema=[{'source': schema_file}],
            version='0.1.0',
            catalog=spec_catalog
        )
        namespace_catalog = NamespaceCatalog()
        namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
        type_map = TypeMap(namespace_catalog)
        type_map.register_container_type(CORE_NAMESPACE, 'BasicBucket', BasicBucket)
        self.manager = BuildManager(type_map)

    def create_specs(self, quantity):
        # Type BasicBucket contains:
        # - [quantity] untyped group
        # - [quantity] untyped dataset
        # - [quantity] untyped array dataset
        # quantity can be only '?' or 1
        untyped_group_spec = GroupSpec(
            doc='A test group specification with no data type',
            name='untyped_group',
            quantity=quantity,
        )
        untyped_dataset_spec = DatasetSpec(
            doc='A test dataset specification with no data type',
            name='untyped_dataset',
            dtype='int',
            quantity=quantity,
        )
        untyped_array_dataset_spec = DatasetSpec(
            doc='A test dataset specification with no data type',
            name='untyped_array_dataset',
            dtype='int',
            dims=[None],
            shape=[None],
            quantity=quantity,
        )
        basic_bucket_spec = GroupSpec(
            doc='A test group specification for a data type containing data type',
            name="test_bucket",
            data_type_def='BasicBucket',
            groups=[untyped_group_spec],
            datasets=[untyped_dataset_spec, untyped_array_dataset_spec],
        )
        return [basic_bucket_spec]


class TestBuildOneUntyped(UntypedMixin, TestCase):
    """Test building a group that has an untyped subgroup/dataset with quantity 1.
    """
    def setUp(self):
        specs = self.create_specs(DEF_QUANTITY)
        self.setUpManager(specs)

    def test_build_data(self):
        """Test building a container which contains an untyped empty subgroup and an untyped non-empty dataset."""
        bucket = BasicBucket(name='test_bucket', untyped_dataset=3, untyped_array_dataset=[3])
        # a required untyped empty group builder will be created by default
        untyped_group_builder = GroupBuilder(name='untyped_group')
        untyped_dataset_builder = DatasetBuilder(name='untyped_dataset', data=3)
        untyped_array_dataset_builder = DatasetBuilder(name='untyped_array_dataset', data=[3])
        bucket_builder = GroupBuilder(
            name='test_bucket',
            groups={'untyped_group': untyped_group_builder},
            datasets={'untyped_dataset': untyped_dataset_builder,
                      'untyped_array_dataset': untyped_array_dataset_builder},
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'BasicBucket',
                        'object_id': bucket.object_id}
        )
        builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)

    def test_build_empty_data(self):
        """Test building a container which contains an untyped empty subgroup and an untyped empty dataset."""
        bucket = BasicBucket(name='test_bucket')
        # a required untyped empty group builder will be created by default
        untyped_group_builder = GroupBuilder(name='untyped_group')
        # a required untyped empty dataset builder will NOT be created by default
        bucket_builder = GroupBuilder(
            name='test_bucket',
            groups={'untyped_group': untyped_group_builder},
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'BasicBucket',
                        'object_id': bucket.object_id}
        )
        msg = "BasicBucket 'test_bucket' is missing required value for attribute 'untyped_dataset'."
        # also raises "BasicBucket 'test_bucket' is missing required value for attribute 'untyped_array_dataset'."
        with self.assertWarnsWith(MissingRequiredBuildWarning, msg):
            builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)


class TestBuildZeroOrOneUntyped(UntypedMixin, TestCase):
    """Test building a group that has an untyped subgroup/dataset with quantity '?'.
    """
    def setUp(self):
        specs = self.create_specs(ZERO_OR_ONE)
        self.setUpManager(specs)

    def test_build_data(self):
        """Test building a container which contains an untyped empty subgroup and an untyped non-empty dataset."""
        bucket = BasicBucket(name='test_bucket', untyped_dataset=3, untyped_array_dataset=[3])
        # an optional untyped empty group builder will NOT be created by default
        untyped_dataset_builder = DatasetBuilder(name='untyped_dataset', data=3)
        untyped_array_dataset_builder = DatasetBuilder(name='untyped_array_dataset', data=[3])
        bucket_builder = GroupBuilder(
            name='test_bucket',
            datasets={'untyped_dataset': untyped_dataset_builder,
                      'untyped_array_dataset': untyped_array_dataset_builder},
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'BasicBucket',
                        'object_id': bucket.object_id}
        )
        builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)

    def test_build_empty_data(self):
        """Test building a container which contains an untyped empty subgroup and an untyped empty dataset."""
        bucket = BasicBucket(name='test_bucket')
        # an optional untyped empty group builder will NOT be created by default
        # an optional untyped empty dataset builder will NOT be created by default
        bucket_builder = GroupBuilder(
            name='test_bucket',
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'BasicBucket',
                        'object_id': bucket.object_id}
        )
        builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)


# Multiple allowed types

class TestBuildMultiTypeInc(BuildQuantityMixin, TestCase):
    """Test build process when a groupspec allows multiple groups/datasets/links with different data types / targets.
    """

    def setUp(self):
        specs = self.create_specs(ZERO_OR_MANY)
        self.setUpManager(specs)

    def create_specs(self, quantity):
        # Type SimpleBucket contains:
        # - [quantity] groups of data_type_inc SimpleFoo and [quantity] group of data_type_inc NotSimpleFoo
        # - [quantity] datasets of data_type_inc SimpleQux and [quantity] datasets of data_type_inc NotSimpleQux
        # - [quantity] links of target_type SimpleFoo and [quantity] links of target_type NotSimpleFoo
        foo_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='SimpleFoo',
        )
        not_foo_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='NotSimpleFoo',
        )
        qux_spec = DatasetSpec(
            doc='A test group specification with a data type',
            data_type_def='SimpleQux',
        )
        not_qux_spec = DatasetSpec(
            doc='A test group specification with a data type',
            data_type_def='NotSimpleQux',
        )
        foo_inc_spec = GroupSpec(
            doc='the SimpleFoos in this bucket',
            data_type_inc='SimpleFoo',
            quantity=quantity
        )
        not_foo_inc_spec = GroupSpec(
            doc='the SimpleFoos in this bucket',
            data_type_inc='NotSimpleFoo',
            quantity=quantity
        )
        qux_inc_spec = DatasetSpec(
            doc='the SimpleQuxs in this bucket',
            data_type_inc='SimpleQux',
            quantity=quantity
        )
        not_qux_inc_spec = DatasetSpec(
            doc='the SimpleQuxs in this bucket',
            data_type_inc='NotSimpleQux',
            quantity=quantity
        )
        foo_link_spec = LinkSpec(
            doc='the links in this bucket',
            target_type='SimpleFoo',
            quantity=quantity
        )
        not_foo_link_spec = LinkSpec(
            doc='the links in this bucket',
            target_type='NotSimpleFoo',
            quantity=quantity
        )
        bucket_spec = GroupSpec(
            doc='A test group specification for a data type containing data type',
            name="test_bucket",
            data_type_def='SimpleBucket',
            groups=[foo_inc_spec, not_foo_inc_spec],
            datasets=[qux_inc_spec, not_qux_inc_spec],
            links=[foo_link_spec, not_foo_link_spec]
        )
        return [foo_spec, not_foo_spec, qux_spec, not_qux_spec, bucket_spec]

    def setUpBucketMapper(self):
        class BucketMapper(ObjectMapper):
            def __init__(self, spec):
                super().__init__(spec)
                self.map_spec('foos', spec.get_data_type('SimpleFoo'))
                self.map_spec('foos', spec.get_data_type('NotSimpleFoo'))
                self.map_spec('quxs', spec.get_data_type('SimpleQux'))
                self.map_spec('quxs', spec.get_data_type('NotSimpleQux'))
                self.map_spec('links', spec.links[0])
                self.map_spec('links', spec.links[1])

        return BucketMapper

    def get_two_bucket_test(self):
        foos = [SimpleFoo('my_foo1'), NotSimpleFoo('my_foo2')]
        quxs = [SimpleQux('my_qux1', data=[1, 2, 3]), NotSimpleQux('my_qux2', data=[4, 5, 6])]
        # NOTE: unlike in the other tests, links cannot map to the same foos in bucket because of a name clash
        links = [SimpleFoo('my_foo3'), NotSimpleFoo('my_foo4')]
        bucket = SimpleBucket(
            name='test_bucket',
            foos=foos,
            quxs=quxs,
            links=links
        )
        foo1_builder = self._create_builder(bucket.foos['my_foo1'])
        foo2_builder = self._create_builder(bucket.foos['my_foo2'])
        foo3_builder = self._create_builder(bucket.links['my_foo3'])
        foo4_builder = self._create_builder(bucket.links['my_foo4'])
        qux1_builder = self._create_builder(bucket.quxs['my_qux1'])
        qux2_builder = self._create_builder(bucket.quxs['my_qux2'])
        foo3_link_builder = LinkBuilder(builder=foo3_builder)
        foo4_link_builder = LinkBuilder(builder=foo4_builder)
        bucket_builder = GroupBuilder(
            name='test_bucket',
            groups={'my_foo1': foo1_builder,
                    'my_foo2': foo2_builder},
            datasets={'my_qux1': qux1_builder,
                      'my_qux2': qux2_builder},
            links={'my_foo3': foo3_link_builder,
                   'my_foo4': foo4_link_builder},
            attributes={'namespace': CORE_NAMESPACE,
                        'data_type': 'SimpleBucket',
                        'object_id': bucket.object_id}
        )
        return bucket, bucket_builder

    def test_build_two(self):
        """Test building a container which contains multiple containers of different types as the spec allows."""
        bucket, bucket_builder = self.get_two_bucket_test()
        builder = self.manager.build(bucket)
        self.assertDictEqual(builder, bucket_builder)
