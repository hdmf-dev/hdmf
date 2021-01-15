import tempfile

from hdmf.build import TypeMap
from hdmf.container import Container
from hdmf.spec import NamespaceCatalog, SpecCatalog, SpecNamespace
from hdmf.utils import docval, getargs

CORE_NAMESPACE = 'test_core'


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


def get_temp_filepath():
    # On Windows, h5py cannot truncate an open file in write mode.
    # The temp file will be closed before h5py truncates it and will be removed during the tearDown step.
    temp_file = tempfile.NamedTemporaryFile()
    temp_file.close()
    return temp_file.name


def create_test_type_map(specs, container_classes, mappers=None):
    """
    Create a TypeMap with the specs registered under a test namespace, and classes and mappers registered to type names.

    :param specs: list of specs
    :param container_classes: dict of type name to container class
    :param mappers: (optional) dict of type name to mapper class
    :return: the constructed TypeMap
    """
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
    for type_name, container_cls in container_classes.items():
        type_map.register_container_type(CORE_NAMESPACE, type_name, container_cls)
    if mappers:
        for type_name, mapper_cls in mappers.items():
            container_cls = container_classes[type_name]
            type_map.register_map(container_cls, mapper_cls)
    return type_map
