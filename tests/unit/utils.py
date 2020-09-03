import tempfile

from hdmf.utils import (docval, getargs)
from hdmf.container import Container
from hdmf.spec.spec import (AttributeSpec, DatasetSpec, GroupSpec, LinkSpec, RefSpec,
                            ZERO_OR_MANY, ONE_OR_MANY, ZERO_OR_ONE)
from hdmf.spec.namespace import (NamespaceCatalog, SpecNamespace)
from hdmf.build import (ObjectMapper, TypeMap, BuildManager)
from hdmf.spec.catalog import SpecCatalog


CORE_NAMESPACE = 'test_core'


class CacheSpecTestHelper(object):

    @staticmethod
    def get_types(catalog):
        types = set()
        for ns_name in catalog.namespaces:
            ns = catalog.get_namespace(ns_name)
            for source in ns['schema']:
                types.update(catalog.get_types(source['source']))
        return types


class Foo(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Foo'},
            {'name': 'my_data', 'type': ('array_data', 'data'), 'doc': 'some data'},
            {'name': 'attr1', 'type': str, 'doc': 'an attribute'},
            {'name': 'attr2', 'type': int, 'doc': 'another attribute'},
            {'name': 'attr3', 'type': float, 'doc': 'a third attribute', 'default': 3.14})
    def __init__(self, **kwargs):
        name, my_data, attr1, attr2, attr3 = getargs('name', 'my_data', 'attr1', 'attr2', 'attr3', kwargs)
        super().__init__(name=name)
        self.__data = my_data
        self.__attr1 = attr1
        self.__attr2 = attr2
        self.__attr3 = attr3

    def __eq__(self, other):
        attrs = ('name', 'my_data', 'attr1', 'attr2', 'attr3')
        return all(getattr(self, a) == getattr(other, a) for a in attrs)

    def __str__(self):
        attrs = ('name', 'my_data', 'attr1', 'attr2', 'attr3')
        return '<' + ','.join('%s=%s' % (a, getattr(self, a)) for a in attrs) + '>'

    @property
    def my_data(self):
        return self.__data

    @property
    def attr1(self):
        return self.__attr1

    @property
    def attr2(self):
        return self.__attr2

    @property
    def attr3(self):
        return self.__attr3

    def __hash__(self):
        return hash(self.name)


class FooBucket(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this bucket'},
            {'name': 'foos', 'type': list, 'doc': 'the Foo objects in this bucket', 'default': list()})
    def __init__(self, **kwargs):
        name, foos = getargs('name', 'foos', kwargs)
        super().__init__(name=name)
        self.__foos = {f.name: f for f in foos}  # note: collections of groups are unordered in HDF5
        for f in foos:
            f.parent = self

    def __eq__(self, other):
        return self.name == other.name and self.foos == other.foos

    def __str__(self):
        return 'name=%s, foos=%s' % (self.name, self.foos)

    @property
    def foos(self):
        return self.__foos

    def remove_foo(self, foo_name):
        foo = self.__foos.pop(foo_name)
        if foo.parent is self:
            self._remove_child(foo)
        return foo


class FooFile(Container):
    """
    NOTE: if the ROOT_NAME for the backend is not 'root' then we must set FooFile.ROOT_NAME before use
          and should be reset to 'root' when use is finished to avoid potential cross-talk between tests.
    """

    ROOT_NAME = 'root'  # For HDF5 and Zarr this is the root. It should be set before use if different for the backend.

    @docval({'name': 'buckets', 'type': list, 'doc': 'the FooBuckets in this file', 'default': list()},
            {'name': 'foo_link', 'type': Foo, 'doc': 'an optional linked Foo', 'default': None},
            {'name': 'foofile_data', 'type': 'array_data', 'doc': 'an optional dataset', 'default': None},
            {'name': 'foo_ref_attr', 'type': Foo, 'doc': 'a reference Foo', 'default': None},
            )
    def __init__(self, **kwargs):
        buckets, foo_link, foofile_data, foo_ref_attr = getargs('buckets', 'foo_link', 'foofile_data',
                                                                'foo_ref_attr', kwargs)
        super().__init__(name=self.ROOT_NAME)  # name is not used - FooFile should be the root container
        self.__buckets = {b.name: b for b in buckets}  # note: collections of groups are unordered in HDF5
        for f in buckets:
            f.parent = self
        self.__foo_link = foo_link
        self.__foofile_data = foofile_data
        self.__foo_ref_attr = foo_ref_attr

    def __eq__(self, other):
        return (self.buckets == other.buckets
                and self.foo_link == other.foo_link
                and self.foofile_data == other.foofile_data)

    def __str__(self):
        return ('buckets=%s, foo_link=%s, foofile_data=%s' % (self.buckets, self.foo_link, self.foofile_data))

    @property
    def buckets(self):
        return self.__buckets

    def add_bucket(self, bucket):
        self.__buckets[bucket.name] = bucket
        bucket.parent = self

    def remove_bucket(self, bucket_name):
        bucket = self.__buckets.pop(bucket_name)
        if bucket.parent is self:
            self._remove_child(bucket)
        return bucket

    @property
    def foo_link(self):
        return self.__foo_link

    @foo_link.setter
    def foo_link(self, value):
        if self.__foo_link is None:
            self.__foo_link = value
        else:
            raise ValueError("can't reset foo_link attribute")

    @property
    def foofile_data(self):
        return self.__foofile_data

    @foofile_data.setter
    def foofile_data(self, value):
        if self.__foofile_data is None:
            self.__foofile_data = value
        else:
            raise ValueError("can't reset foofile_data attribute")

    @property
    def foo_ref_attr(self):
        return self.__foo_ref_attr

    @foo_ref_attr.setter
    def foo_ref_attr(self, value):
        if self.__foo_ref_attr is None:
            self.__foo_ref_attr = value
        else:
            raise ValueError("can't reset foo_ref_attr attribute")


def get_foo_buildmanager():
    """
    Get a BuildManager (and create all ObjectMappers) for a foofile
    :return:
    """

    foo_spec = GroupSpec('A test group specification with a data type',
                         data_type_def='Foo',
                         datasets=[DatasetSpec('an example dataset',
                                               'int',
                                               name='my_data',
                                               attributes=[AttributeSpec('attr2',
                                                                         'an example integer attribute',
                                                                         'int')])],
                         attributes=[AttributeSpec('attr1', 'an example string attribute', 'text'),
                                     AttributeSpec('attr3', 'an example float attribute', 'float')])

    tmp_spec = GroupSpec('A subgroup for Foos',
                         name='foo_holder',
                         groups=[GroupSpec('the Foos in this bucket', data_type_inc='Foo', quantity=ZERO_OR_MANY)])

    bucket_spec = GroupSpec('A test group specification for a data type containing data type',
                            data_type_def='FooBucket',
                            groups=[tmp_spec])

    class FooMapper(ObjectMapper):
        def __init__(self, spec):
            super().__init__(spec)
            my_data_spec = spec.get_dataset('my_data')
            self.map_spec('attr2', my_data_spec.get_attribute('attr2'))

    class BucketMapper(ObjectMapper):
        def __init__(self, spec):
            super().__init__(spec)
            foo_holder_spec = spec.get_group('foo_holder')
            self.unmap(foo_holder_spec)
            foo_spec = foo_holder_spec.get_data_type('Foo')
            self.map_spec('foos', foo_spec)

    file_links_spec = GroupSpec('Foo link group',
                                name='links',
                                links=[LinkSpec('Foo link',
                                                name='foo_link',
                                                target_type='Foo',
                                                quantity=ZERO_OR_ONE)]
                                )

    file_spec = GroupSpec("A file of Foos contained in FooBuckets",
                          data_type_def='FooFile',
                          groups=[GroupSpec('Holds the FooBuckets',
                                            name='buckets',
                                            groups=[GroupSpec("One or more FooBuckets",
                                                              data_type_inc='FooBucket',
                                                              quantity=ONE_OR_MANY)]),
                                  file_links_spec],
                          datasets=[DatasetSpec('Foo data',
                                                name='foofile_data',
                                                dtype='int',
                                                quantity=ZERO_OR_ONE)],
                          attributes=[AttributeSpec(doc='Foo ref attr',
                                                    name='foo_ref_attr',
                                                    dtype=RefSpec('Foo', 'object'),
                                                    required=False)],
                          )

    class FileMapper(ObjectMapper):
        def __init__(self, spec):
            super().__init__(spec)
            bucket_spec = spec.get_group('buckets').get_data_type('FooBucket')
            self.map_spec('buckets', bucket_spec)
            self.unmap(spec.get_group('links'))
            foo_link_spec = spec.get_group('links').get_link('foo_link')
            self.map_spec('foo_link', foo_link_spec)

    spec_catalog = SpecCatalog()
    spec_catalog.register_spec(foo_spec, 'test.yaml')
    spec_catalog.register_spec(bucket_spec, 'test.yaml')
    spec_catalog.register_spec(file_spec, 'test.yaml')
    namespace = SpecNamespace(
        'a test namespace',
        CORE_NAMESPACE,
        [{'source': 'test.yaml'}],
        version='0.1.0',
        catalog=spec_catalog)
    namespace_catalog = NamespaceCatalog()
    namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
    type_map = TypeMap(namespace_catalog)

    type_map.register_container_type(CORE_NAMESPACE, 'Foo', Foo)
    type_map.register_container_type(CORE_NAMESPACE, 'FooBucket', FooBucket)
    type_map.register_container_type(CORE_NAMESPACE, 'FooFile', FooFile)

    type_map.register_map(Foo, FooMapper)
    type_map.register_map(FooBucket, BucketMapper)
    type_map.register_map(FooFile, FileMapper)

    manager = BuildManager(type_map)
    return manager


def get_temp_filepath():
    # On Windows, h5py cannot truncate an open file in write mode.
    # The temp file will be closed before h5py truncates it and will be removed during the tearDown step.
    temp_file = tempfile.NamedTemporaryFile()
    temp_file.close()
    return temp_file.name
