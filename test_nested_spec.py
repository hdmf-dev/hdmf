import os
import unittest2 as unittest
import tempfile
import warnings
import numpy as np

from hdmf.utils import docval, getargs
from hdmf.data_utils import DataChunkIterator
from hdmf.backends.hdf5.h5tools import HDF5IO
from hdmf.backends.hdf5 import H5DataIO
from hdmf.build import DatasetBuilder, BuildManager, TypeMap, ObjectMapper
from hdmf.spec.namespace import NamespaceCatalog
from hdmf.spec.spec import AttributeSpec, DatasetSpec, GroupSpec, ZERO_OR_MANY, ONE_OR_MANY
from hdmf.spec.namespace import SpecNamespace
from hdmf.spec.catalog import SpecCatalog
from hdmf.container import Container
from h5py import SoftLink, HardLink, ExternalLink, File

from tests.unit.test_utils import Foo, FooBucket, CORE_NAMESPACE
from tests.unit.test_io_hdf5_h5tools import FooFile

foo_spec = GroupSpec('A test group specification with a data type',
                     data_type_def='Foo',
                     datasets=[DatasetSpec('an example dataset',
                                           'int',
                                           name='my_data',
                                           attributes=[AttributeSpec('attr2',
                                                                     'an example integer attribute',
                                                                     'int')])],
                     attributes=[AttributeSpec('attr1', 'an example string attribute', 'text')])

tmp_spec = GroupSpec('A subgroup for Foos',
                     name='foo_holder',
                     groups=[GroupSpec('the Foos in this bucket', data_type_inc='Foo', quantity=ZERO_OR_MANY)])

bucket_spec = GroupSpec('A test group specification for a data type containing data type',
                        data_type_def='FooBucket',
                        groups=[tmp_spec])

class BucketMapper(ObjectMapper):
    def __init__(self, spec):
        super(BucketMapper, self).__init__(spec)
        foo_spec = spec.get_group('foo_holder').get_data_type('Foo')
        self.map_spec('foos', foo_spec)

file_spec = GroupSpec("A file of Foos contained in FooBuckets",
                      name='root',
                      data_type_def='FooFile',
                      groups=[GroupSpec('Holds the FooBuckets',
                                        name='buckets',
                                        groups=[GroupSpec("One ore more FooBuckets",
                                                          data_type_inc='FooBucket',
                                                          quantity=ONE_OR_MANY)])])

class FileMapper(ObjectMapper):
    def __init__(self, spec):
        super(FileMapper, self).__init__(spec)
        bucket_spec = spec.get_group('buckets').get_data_type('FooBucket')
        self.map_spec('buckets', bucket_spec)

spec_catalog = SpecCatalog()
spec_catalog.register_spec(foo_spec, 'test.yaml')
spec_catalog.register_spec(bucket_spec, 'test.yaml')
spec_catalog.register_spec(file_spec, 'test.yaml')
namespace = SpecNamespace(
    'a test namespace',
    CORE_NAMESPACE,
    [{'source': 'test.yaml'}],
    catalog=spec_catalog)
namespace_catalog = NamespaceCatalog()
namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
type_map = TypeMap(namespace_catalog)

type_map.register_container_type(CORE_NAMESPACE, 'Foo', Foo)
type_map.register_container_type(CORE_NAMESPACE, 'FooBucket', FooBucket)
type_map.register_container_type(CORE_NAMESPACE, 'FooFile', FooFile)

type_map.register_map(FooBucket, BucketMapper)
type_map.register_map(FooFile, FileMapper)

manager = BuildManager(type_map)

spec_catalog = manager.namespace_catalog.get_namespace(CORE_NAMESPACE).catalog
foo_spec = spec_catalog.get_spec('Foo')
# Baz1 class contains an object of Baz2 class
baz_spec2 = GroupSpec('A composition inside',
                      data_type_def='Baz2',
                      data_type_inc=foo_spec,
                      attributes=[
                          AttributeSpec('attr3', 'an example float attribute', 'float'),
                          AttributeSpec('attr4', 'another example float attribute', 'float')])

baz_spec1 = GroupSpec('A composition test outside',
                      data_type_def='Baz1',
                      data_type_inc=foo_spec,
                      attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                  AttributeSpec('attr4', 'another example float attribute', 'float')],
                      groups=[baz_spec2])

# add directly into the existing spec_catalog. would not do this normally.
spec_catalog.register_spec(baz_spec1, 'test.yaml')

Baz2 = manager.type_map.get_container_cls(CORE_NAMESPACE, 'Baz2')
