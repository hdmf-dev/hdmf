import numpy as np
from hdmf.build import ObjectMapper, BuildManager, TypeMap, CustomClassGenerator
from hdmf.container import Container, MultiContainerInterface
from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec, SpecCatalog, SpecNamespace, NamespaceCatalog, LinkSpec
from hdmf.testing import TestCase
from hdmf.utils import get_docval

from .test_io_map import Bar
from tests.unit.utils import CORE_NAMESPACE


class TestDynamicContainer(TestCase):

    def setUp(self):
        self.bar_spec = GroupSpec('A test group specification with a data type',
                                  data_type_def='Bar',
                                  datasets=[DatasetSpec('a dataset', 'int', name='data',
                                                        attributes=[AttributeSpec(
                                                            'attr2', 'an integer attribute', 'int')])],
                                  attributes=[AttributeSpec('attr1', 'a string attribute', 'text')])
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
                             attributes=[AttributeSpec('attr3', 'a float attribute', 'float'),
                                         AttributeSpec('attr4', 'another float attribute', 'float')])
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
                             attributes=[AttributeSpec('attr4', 'another float attribute', 'float')])
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        cls = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')
        inst = cls(attr4=10.)
        self.assertEqual(inst.name, 'bingo')

    def test_dynamic_container_creation_defaults(self):
        baz_spec = GroupSpec('A test extension with no Container class',
                             data_type_def='Baz', data_type_inc=self.bar_spec,
                             attributes=[AttributeSpec('attr3', 'a float attribute', 'float'),
                                         AttributeSpec('attr4', 'another float attribute', 'float')])
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
                             attributes=[AttributeSpec('attr3', 'a float attribute', 'float'),
                                         AttributeSpec('attr4', 'another float attribute', 'float')])
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
                             attributes=[AttributeSpec('attr3', 'a float attribute', 'float'),
                                         AttributeSpec('attr4', 'another float attribute', 'float')])
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
                                 attributes=[AttributeSpec('attr3', 'a float attribute', 'float'),
                                             AttributeSpec('attr4', 'another float attribute', 'float')])
            self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
            cls = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')

            inst = cls([1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0)
            self.assertEqual(inst.name, 'A fixed name')

    def test_dynamic_container_composition(self):
        baz_spec2 = GroupSpec('A composition inside', data_type_def='Baz2',
                              data_type_inc=self.bar_spec,
                              attributes=[
                                  AttributeSpec('attr3', 'a float attribute', 'float'),
                                  AttributeSpec('attr4', 'another float attribute', 'float')])

        baz_spec1 = GroupSpec('A composition test outside', data_type_def='Baz1', data_type_inc=self.bar_spec,
                              attributes=[AttributeSpec('attr3', 'a float attribute', 'float'),
                                          AttributeSpec('attr4', 'another float attribute', 'float')],
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
                                  AttributeSpec('attr3', 'a float attribute', 'float'),
                                  AttributeSpec('attr4', 'another float attribute', 'float')])

        baz_spec1 = GroupSpec('A composition test outside', data_type_def='Baz1', data_type_inc=self.bar_spec,
                              attributes=[AttributeSpec('attr3', 'a float attribute', 'float'),
                                          AttributeSpec('attr4', 'another float attribute', 'float')],
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
                              attributes=[AttributeSpec('attr3', 'a float attribute', 'float'),
                                          AttributeSpec('attr4', 'another float attribute', 'float')],
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
                AttributeSpec('attr3', 'a float attribute', 'float')]
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


class EmptyBar(Container):
    pass


class TestBuildDocval(TestCase):

    def setUp(self):
        self.bar_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='EmptyBar'
        )
        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.bar_spec, 'test.yaml')
        self.namespace = SpecNamespace('a test namespace', CORE_NAMESPACE,
                                       [{'source': 'test.yaml'}],
                                       version='0.1.0',
                                       catalog=self.spec_catalog)
        self.namespace_catalog = NamespaceCatalog()
        self.namespace_catalog.add_namespace(CORE_NAMESPACE, self.namespace)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'EmptyBar', EmptyBar)

    def test_build_docval(self):
        """Test update_docval_args for a variety of data types and mapping configurations."""
        spec = GroupSpec(
            doc="A test group specification with a data type",
            data_type_def="Baz",
            groups=[
                GroupSpec(doc="a group", data_type_inc="EmptyBar", quantity="?")
            ],
            datasets=[
                DatasetSpec(
                    doc="a dataset",
                    dtype="int",
                    name="data",
                    attributes=[
                        AttributeSpec(name="attr2", doc="an integer attribute", dtype="int")
                    ],
                )
            ],
            attributes=[
                AttributeSpec(name="attr1", doc="a string attribute", dtype="text"),
                AttributeSpec(name="attr3", doc="a numeric attribute", dtype="numeric"),
                AttributeSpec(name="attr4", doc="a float attribute", dtype="float"),
            ],
        )

        expected = [
            {'name': 'data', 'type': (int, np.int32, np.int64), 'doc': 'a dataset'},
            {'name': 'attr1', 'type': str, 'doc': 'a string attribute'},
            {'name': 'attr2', 'type': (int, np.int32, np.int64), 'doc': 'an integer attribute'},
            {'name': 'attr3', 'doc': 'a numeric attribute',
             'type': (float, np.float32, np.float64, np.int8, np.int16,
                      np.int32, np.int64, int, np.uint8, np.uint16,
                      np.uint32, np.uint64)},
            {'name': 'attr4', 'doc': 'a float attribute',
             'type': (float, np.float32, np.float64)},
            {'name': 'bar', 'type': EmptyBar, 'doc': 'a group', 'default': None},
        ]

        not_inherited_fields = {
            'data': spec.get_dataset('data'),
            'attr1': spec.get_attribute('attr1'),
            'attr2': spec.get_dataset('data').get_attribute('attr2'),
            'attr3': spec.get_attribute('attr3'),
            'attr4': spec.get_attribute('attr4'),
            'bar': spec.groups[0]
        }

        docval_args = list()
        for i, attr_name in enumerate(not_inherited_fields):
            with self.subTest(attr_name=attr_name):
                CustomClassGenerator.process_field_spec(
                    classdict={},
                    docval_args=docval_args,
                    parent_cls=EmptyBar,  # <-- arbitrary class
                    attr_name=attr_name,
                    not_inherited_fields=not_inherited_fields,
                    type_map=self.type_map
                )
                self.assertListEqual(docval_args, expected[:(i+1)])  # compare with the first i elements of expected

    def test_update_docval_shape(self):
        """Test that update_docval_args for a field with shape sets the shape key."""
        spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Baz',
            attributes=[
                AttributeSpec(name='attr1', doc='a string attribute', dtype='text', shape=[None])
            ]
        )
        not_inherited_fields = {'attr1': spec.get_attribute('attr1')}

        docval_args = list()
        CustomClassGenerator.process_field_spec(
            classdict={},
            docval_args=docval_args,
            parent_cls=EmptyBar,  # <-- arbitrary class
            attr_name='attr1',
            not_inherited_fields=not_inherited_fields,
            type_map=TypeMap()
        )

        expected = [{'name': 'attr1', 'type': ('array_data', 'data'), 'doc': 'a string attribute', 'shape': [None]}]
        self.assertListEqual(docval_args, expected)

    def test_update_docval_default_value(self):
        """Test that update_docval_args for an optional field with default value sets the default key."""
        spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Baz',
            attributes=[
                AttributeSpec(name='attr1', doc='a string attribute', dtype='text', required=False,
                              default_value='value')
            ]
        )
        not_inherited_fields = {'attr1': spec.get_attribute('attr1')}

        docval_args = list()
        CustomClassGenerator.process_field_spec(
            classdict={},
            docval_args=docval_args,
            parent_cls=EmptyBar,  # <-- arbitrary class
            attr_name='attr1',
            not_inherited_fields=not_inherited_fields,
            type_map=TypeMap()
        )

        expected = [{'name': 'attr1', 'type': str, 'doc': 'a string attribute', 'default': 'value'}]
        self.assertListEqual(docval_args, expected)

    def test_update_docval_default_value_none(self):
        """Test that update_docval_args for an optional field sets default: None."""
        spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Baz',
            attributes=[
                AttributeSpec(name='attr1', doc='a string attribute', dtype='text', required=False)
            ]
        )
        not_inherited_fields = {'attr1': spec.get_attribute('attr1')}

        docval_args = list()
        CustomClassGenerator.process_field_spec(
            classdict={},
            docval_args=docval_args,
            parent_cls=EmptyBar,  # <-- arbitrary class
            attr_name='attr1',
            not_inherited_fields=not_inherited_fields,
            type_map=TypeMap()
        )

        expected = [{'name': 'attr1', 'type': str, 'doc': 'a string attribute', 'default': None}]
        self.assertListEqual(docval_args, expected)

    def test_process_field_spec_overwrite(self):
        """Test that docval generation overwrites previous docval args."""
        spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Baz',
            attributes=[
                AttributeSpec(name='attr1', doc='a string attribute', dtype='text', shape=[None])
            ]
        )
        not_inherited_fields = {'attr1': spec.get_attribute('attr1')}

        docval_args = [{'name': 'attr1', 'type': ('array_data', 'data'), 'doc': 'a string attribute',
                        'shape': [[None], [None, None]]},  # this dict will be overwritten below
                       {'name': 'attr2', 'type': ('array_data', 'data'), 'doc': 'a string attribute',
                        'shape': [[None], [None, None]]}]
        CustomClassGenerator.process_field_spec(
            classdict={},
            docval_args=docval_args,
            parent_cls=EmptyBar,  # <-- arbitrary class
            attr_name='attr1',
            not_inherited_fields=not_inherited_fields,
            type_map=TypeMap()
        )

        expected = [{'name': 'attr1', 'type': ('array_data', 'data'), 'doc': 'a string attribute',
                    'shape': [None]},
                    {'name': 'attr2', 'type': ('array_data', 'data'), 'doc': 'a string attribute',
                     'shape': [[None], [None, None]]}]
        self.assertListEqual(docval_args, expected)

    def test_process_field_spec_link(self):
        """Test that processing a link spec does not set child=True in __fields__."""
        classdict = {}
        not_inherited_fields = {'attr3': LinkSpec(name='attr3', target_type='EmptyBar', doc='a link')}
        CustomClassGenerator.process_field_spec(
            classdict=classdict,
            docval_args=[],
            parent_cls=EmptyBar,  # <-- arbitrary class
            attr_name='attr3',
            not_inherited_fields=not_inherited_fields,
            type_map=self.type_map
        )

        expected = {'__fields__': [{'name': 'attr3', 'doc': 'a link'}]}
        self.assertDictEqual(classdict, expected)

    def test_post_process_fixed_name(self):
        """Test that docval generation for a class with a fixed name does not contain a docval arg for name."""
        spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Baz',
            name='MyBaz',  # <-- fixed name
            attributes=[
                AttributeSpec(
                    name='attr1',
                    doc='a string attribute',
                    dtype='text',
                    shape=[None]
                )
            ]
        )

        docval_args = [{'name': 'name', 'type': str, 'doc': 'name'},
                       {'name': 'attr1', 'type': ('array_data', 'data'), 'doc': 'a string attribute',
                        'shape': [None]}]
        CustomClassGenerator.post_process({}, [], docval_args, spec)

        expected = [{'name': 'attr1', 'type': ('array_data', 'data'), 'doc': 'a string attribute',
                     'shape': [None]}]
        self.assertListEqual(docval_args, expected)

    def test_post_process_default_name(self):
        """Test that docval generation for a class with a default name has the default value for name set."""
        spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Baz',
            default_name='MyBaz',  # <-- default name
            attributes=[
                AttributeSpec(
                    name='attr1',
                    doc='a string attribute',
                    dtype='text',
                    shape=[None]
                )
            ]
        )

        docval_args = [{'name': 'name', 'type': str, 'doc': 'name'},
                       {'name': 'attr1', 'type': ('array_data', 'data'), 'doc': 'a string attribute',
                        'shape': [None]}]
        CustomClassGenerator.post_process({}, [], docval_args, spec)

        expected = [{'name': 'name', 'type': str, 'doc': 'name', 'default': 'MyBaz'},
                    {'name': 'attr1', 'type': ('array_data', 'data'), 'doc': 'a string attribute',
                     'shape': [None]}]
        self.assertListEqual(docval_args, expected)
