import h5py
import numpy as np
from hdmf.build import ObjectMapper, BuildManager, TypeMap, CustomClassGenerator
from hdmf.container import MultiContainerInterface
from hdmf.data_utils import DataIO, AbstractDataChunkIterator
from hdmf.query import HDMFDataset
from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec, SpecCatalog, SpecNamespace, NamespaceCatalog, LinkSpec
from hdmf.testing import TestCase
from hdmf.utils import get_docval

from .test_io_map import Bar
from tests.unit.utils import CORE_NAMESPACE


class TestDynamicContainer(TestCase):

    def setUp(self):
        self.bar_spec = GroupSpec('A test group specification with a data type',
                                  data_type_def='Bar',
                                  datasets=[DatasetSpec('an example dataset', 'int', name='data',
                                                        attributes=[AttributeSpec(
                                                            'attr2', 'an example integer attribute', 'int')])],
                                  attributes=[AttributeSpec('attr1', 'an example string attribute', 'text')])
        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.bar_spec, 'test.yaml')
        self.namespace = SpecNamespace('a test namespace', CORE_NAMESPACE,
                                       [{'source': 'test.yaml'}],
                                       version='0.1.0',
                                       catalog=self.spec_catalog)
        self.namespace_catalog = NamespaceCatalog()
        self.namespace_catalog.add_namespace(CORE_NAMESPACE, self.namespace)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Bar', Bar)
        self.manager = BuildManager(self.type_map)
        self.mapper = ObjectMapper(self.bar_spec)

    def test_dynamic_container_creation(self):
        baz_spec = GroupSpec('A test extension with no Container class',
                             data_type_def='Baz', data_type_inc=self.bar_spec,
                             attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                         AttributeSpec('attr4', 'another example float attribute', 'float')])
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        cls = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')
        expected_args = {'name', 'data', 'attr1', 'attr2', 'attr3', 'attr4'}
        received_args = set()
        for x in get_docval(cls.__init__):
            if x['name'] != 'foo':
                received_args.add(x['name'])
                with self.subTest(name=x['name']):
                    self.assertNotIn('default', x)
        self.assertSetEqual(expected_args, received_args)
        self.assertEqual(cls.__name__, 'Baz')
        self.assertTrue(issubclass(cls, Bar))

    def test_dynamic_container_default_name(self):
        baz_spec = GroupSpec('doc', default_name='bingo', data_type_def='Baz',
                             attributes=[AttributeSpec('attr4', 'another example float attribute', 'float')])
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        cls = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')
        inst = cls(attr4=10.)
        self.assertEqual(inst.name, 'bingo')

    def test_dynamic_container_creation_defaults(self):
        baz_spec = GroupSpec('A test extension with no Container class',
                             data_type_def='Baz', data_type_inc=self.bar_spec,
                             attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                         AttributeSpec('attr4', 'another example float attribute', 'float')])
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        cls = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')
        expected_args = {'name', 'data', 'attr1', 'attr2', 'attr3', 'attr4', 'foo'}
        received_args = set(map(lambda x: x['name'], get_docval(cls.__init__)))
        self.assertSetEqual(expected_args, received_args)
        self.assertEqual(cls.__name__, 'Baz')
        self.assertTrue(issubclass(cls, Bar))

    def test_dynamic_container_constructor(self):
        baz_spec = GroupSpec('A test extension with no Container class',
                             data_type_def='Baz', data_type_inc=self.bar_spec,
                             attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                         AttributeSpec('attr4', 'another example float attribute', 'float')])
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        cls = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')
        # TODO: test that constructor works!
        inst = cls('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0)
        self.assertEqual(inst.name, 'My Baz')
        self.assertEqual(inst.data, [1, 2, 3, 4])
        self.assertEqual(inst.attr1, 'string attribute')
        self.assertEqual(inst.attr2, 1000)
        self.assertEqual(inst.attr3, 98.6)
        self.assertEqual(inst.attr4, 1.0)

    def test_dynamic_container_constructor_name(self):
        # name is specified in spec and cannot be changed
        baz_spec = GroupSpec('A test extension with no Container class',
                             data_type_def='Baz', data_type_inc=self.bar_spec,
                             name='A fixed name',
                             attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                         AttributeSpec('attr4', 'another example float attribute', 'float')])
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        cls = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')

        with self.assertRaises(TypeError):
            inst = cls('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0)

        inst = cls([1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0)
        self.assertEqual(inst.name, 'A fixed name')
        self.assertEqual(inst.data, [1, 2, 3, 4])
        self.assertEqual(inst.attr1, 'string attribute')
        self.assertEqual(inst.attr2, 1000)
        self.assertEqual(inst.attr3, 98.6)
        self.assertEqual(inst.attr4, 1.0)

    def test_dynamic_container_constructor_name_default_name(self):
        # if both name and default_name are specified, name should be used
        with self.assertWarns(Warning):
            baz_spec = GroupSpec('A test extension with no Container class',
                                 data_type_def='Baz', data_type_inc=self.bar_spec,
                                 name='A fixed name',
                                 default_name='A default name',
                                 attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                             AttributeSpec('attr4', 'another example float attribute', 'float')])
            self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
            cls = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')

            inst = cls([1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0)
            self.assertEqual(inst.name, 'A fixed name')

    def test_dynamic_container_composition(self):
        baz_spec2 = GroupSpec('A composition inside', data_type_def='Baz2',
                              data_type_inc=self.bar_spec,
                              attributes=[
                                  AttributeSpec('attr3', 'an example float attribute', 'float'),
                                  AttributeSpec('attr4', 'another example float attribute', 'float')])

        baz_spec1 = GroupSpec('A composition test outside', data_type_def='Baz1', data_type_inc=self.bar_spec,
                              attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                          AttributeSpec('attr4', 'another example float attribute', 'float')],
                              groups=[GroupSpec('A composition inside', data_type_inc='Baz2')])
        self.spec_catalog.register_spec(baz_spec1, 'extension.yaml')
        self.spec_catalog.register_spec(baz_spec2, 'extension.yaml')
        Baz2 = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz2')
        Baz1 = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz1')
        Baz1('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0,
             baz2=Baz2('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0))

        Bar = self.type_map.get_container_cls(CORE_NAMESPACE, 'Bar')
        bar = Bar('My Bar', [1, 2, 3, 4], 'string attribute', 1000)

        with self.assertRaises(TypeError):
            Baz1('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0, baz2=bar)

    def test_dynamic_container_composition_reverse_order(self):
        baz_spec2 = GroupSpec('A composition inside', data_type_def='Baz2',
                              data_type_inc=self.bar_spec,
                              attributes=[
                                  AttributeSpec('attr3', 'an example float attribute', 'float'),
                                  AttributeSpec('attr4', 'another example float attribute', 'float')])

        baz_spec1 = GroupSpec('A composition test outside', data_type_def='Baz1', data_type_inc=self.bar_spec,
                              attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                          AttributeSpec('attr4', 'another example float attribute', 'float')],
                              groups=[GroupSpec('A composition inside', data_type_inc='Baz2')])
        self.spec_catalog.register_spec(baz_spec1, 'extension.yaml')
        self.spec_catalog.register_spec(baz_spec2, 'extension.yaml')
        Baz1 = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz1')
        Baz2 = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz2')
        Baz1('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0,
             baz2=Baz2('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0))

        Bar = self.type_map.get_container_cls(CORE_NAMESPACE, 'Bar')
        bar = Bar('My Bar', [1, 2, 3, 4], 'string attribute', 1000)

        with self.assertRaises(TypeError):
            Baz1('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0, baz2=bar)

    def test_dynamic_container_composition_missing_type(self):
        baz_spec1 = GroupSpec('A composition test outside', data_type_def='Baz1', data_type_inc=self.bar_spec,
                              attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                          AttributeSpec('attr4', 'another example float attribute', 'float')],
                              groups=[GroupSpec('A composition inside', data_type_inc='Baz2')])
        self.spec_catalog.register_spec(baz_spec1, 'extension.yaml')

        msg = "No specification for 'Baz2' in namespace 'test_core'"
        with self.assertRaisesWith(ValueError, msg):
            self.manager.type_map.get_container_cls(CORE_NAMESPACE, 'Baz1')

    def test_dynamic_container_fixed_name(self):
        """Test that dynamic class generation for an extended type with a fixed name works."""
        baz_spec = GroupSpec('A test extension with no Container class',
                             data_type_def='Baz', data_type_inc=self.bar_spec, name='Baz')
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        Baz = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')
        obj = Baz([1, 2, 3, 4], 'string attribute', attr2=1000)
        self.assertEqual(obj.name, 'Baz')

    def test_multi_container_spec(self):
        multi_spec = GroupSpec(
            'A test extension that contains a multi',
            data_type_def='Multi',
            groups=[
                GroupSpec(
                    data_type_inc=self.bar_spec,
                    doc='test multi',
                    quantity='*')],
            attributes=[
                AttributeSpec('attr3', 'an example float attribute', 'float')]
        )
        self.spec_catalog.register_spec(multi_spec, 'extension.yaml')
        Bar = self.type_map.get_container_cls(CORE_NAMESPACE, 'Bar')
        Multi = self.type_map.get_container_cls(CORE_NAMESPACE, 'Multi')
        assert issubclass(Multi, MultiContainerInterface)
        assert Multi.__clsconf__[0] == dict(
            attr='bars',
            type=Bar,
            add='add_bars',
            get='get_bars',
            create='create_bars'
        )

        multi = Multi(name='my_multi',
                      bars=[Bar('my_bar', list(range(10)), 'value1', 10)],
                      attr3=5.)
        assert multi.bars['my_bar'] == Bar('my_bar', list(range(10)), 'value1', 10)
        assert multi.attr3 == 5.

    def test_build_docval(self):
        Bar = self.type_map.get_container_cls(CORE_NAMESPACE, 'Bar')
        addl_fields = dict(
            attr3=AttributeSpec('attr3', 'an example numeric attribute', 'numeric'),
            attr4=AttributeSpec('attr4', 'another example float attribute', 'float')
        )
        docval = CustomClassGenerator._build_docval(Bar, addl_fields, self.type_map, name=None, default_name=None)

        expected = [
            {'doc': 'the name of this Bar', 'name': 'name', 'type': str},
            {'name': 'data',
             'type': (DataIO, np.ndarray, list, tuple, h5py.Dataset,
                      HDMFDataset, AbstractDataChunkIterator),
             'doc': 'some data'},
            {'name': 'attr1', 'type': str,
             'doc': 'an attribute'},
            {'name': 'attr2', 'type': int,
             'doc': 'another attribute'},
            {'name': 'attr3', 'doc': 'an example numeric attribute',
             'type': (float, np.float32, np.float64, np.int8, np.int16,
                      np.int32, np.int64, int, np.uint8, np.uint16,
                      np.uint32, np.uint64)},
            {'name': 'foo', 'type': 'Foo', 'doc': 'a group', 'default': None},
            {'name': 'attr4', 'doc': 'another example float attribute',
             'type': (float, np.float32, np.float64)}
        ]

        self.assertListEqual(docval, expected)

    def test_build_docval_shape(self):
        """Test that docval generation for a class with shape has the shape set."""
        Bar = self.type_map.get_container_cls(CORE_NAMESPACE, 'Bar')
        addl_fields = dict(attr3=AttributeSpec('attr3', 'an example numeric attribute', 'numeric', shape=[None]))
        docval = CustomClassGenerator._build_docval(Bar, addl_fields, self.type_map, name=None, default_name=None)

        for arg in docval:
            if arg['name'] == 'attr3':
                self.assertListEqual(arg['shape'], [None])

    def test_build_docval_default_value(self):
        """Test that docval generation for a class with an additional optional field has the default value set."""
        Bar = self.type_map.get_container_cls(CORE_NAMESPACE, 'Bar')
        addl_fields = dict(attr3=AttributeSpec('attr3', 'an example numeric attribute', 'float',
                                               required=False, default_value=10.0))
        docval = CustomClassGenerator._build_docval(Bar, addl_fields, self.type_map, name=None, default_name=None)

        for arg in docval:
            if arg['name'] == 'attr3':
                self.assertEqual(arg['default'], 10.0)

    def test_build_docval_default_value_none(self):
        """Test that docval generation for a class with an additional optional field has default: None."""
        Bar = self.type_map.get_container_cls(CORE_NAMESPACE, 'Bar')
        addl_fields = dict(attr3=AttributeSpec('attr3', 'an example numeric attribute', 'float',
                                               required=False))
        docval = CustomClassGenerator._build_docval(Bar, addl_fields, self.type_map, name=None, default_name=None)

        for arg in docval:
            if arg['name'] == 'attr3':
                self.assertIsNone(arg['default'])

    def test_build_docval_fixed_name(self):
        """Test that docval generation for a class with a fixed name does not contain a docval arg for name."""
        docval = CustomClassGenerator._build_docval(Bar, {}, self.type_map, name='Baz', default_name=None)

        found = False
        for arg in docval:
            if arg['name'] == 'name':
                found = True
        self.assertFalse(found)

    def test_build_docval_default_name(self):
        """Test that docval generation for a class with a default name has the default value for name set."""
        docval = CustomClassGenerator._build_docval(Bar, {}, self.type_map, name=None, default_name='MyBaz')

        for arg in docval:
            if arg['name'] == 'name':
                self.assertEqual(arg['default'], 'MyBaz')

    def test_build_docval_link(self):
        Bar = self.type_map.get_container_cls(CORE_NAMESPACE, 'Bar')
        addl_fields = dict(
            attr3=LinkSpec(name='attr3', target_type='Bar', doc='an example link'),
        )
        docval = CustomClassGenerator._build_docval(Bar, addl_fields, self.type_map, name=None, default_name=None)

        for arg in docval:
            if arg['name'] == 'attr3':
                self.assertIs(arg['type'], Bar)
