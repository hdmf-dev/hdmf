import numpy as np
import os
import shutil
import tempfile

from hdmf.build import TypeMap, CustomClassGenerator
from hdmf.build.classgenerator import ClassGenerator, MCIClassGenerator
from hdmf.container import Container, Data, MultiContainerInterface, AbstractContainer
from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec, SpecCatalog, SpecNamespace, NamespaceCatalog, LinkSpec
from hdmf.testing import TestCase
from hdmf.utils import get_docval

from .test_io_map import Bar
from tests.unit.utils import CORE_NAMESPACE, create_test_type_map, create_load_namespace_yaml


class TestClassGenerator(TestCase):

    def test_register_generator(self):
        """Test TypeMap.register_generator and ClassGenerator.register_generator."""

        class MyClassGenerator(CustomClassGenerator):

            @classmethod
            def apply_generator_to_field(cls, field_spec, bases, type_map):
                return True

            @classmethod
            def process_field_spec(cls, classdict, docval_args, parent_cls, attr_name, not_inherited_fields, type_map,
                                   spec):
                # append attr_name to classdict['__custom_fields__'] list
                classdict.setdefault('process_field_spec', list()).append(attr_name)

            @classmethod
            def post_process(cls, classdict, bases, docval_args, spec):
                classdict['post_process'] = True

        spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Baz',
            attributes=[
                AttributeSpec(name='attr1', doc='a string attribute', dtype='text')
            ]
        )

        spec_catalog = SpecCatalog()
        spec_catalog.register_spec(spec, 'test.yaml')
        namespace = SpecNamespace(
            doc='a test namespace',
            name=CORE_NAMESPACE,
            schema=[{'source': 'test.yaml'}],
            version='0.1.0',
            catalog=spec_catalog
        )
        namespace_catalog = NamespaceCatalog()
        namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
        type_map = TypeMap(namespace_catalog)
        type_map.register_generator(MyClassGenerator)
        cls = type_map.get_dt_container_cls('Baz', CORE_NAMESPACE)

        self.assertEqual(cls.process_field_spec, ['attr1'])
        self.assertTrue(cls.post_process)

    def test_bad_generator(self):
        """Test that register_generator raises an error if the generator is not an instance of CustomClassGenerator."""

        class NotACustomClassGenerator:
            pass

        type_map = TypeMap()

        msg = 'Generator <.*> must be a subclass of CustomClassGenerator.'
        with self.assertRaisesRegex(ValueError, msg):
            type_map.register_generator(NotACustomClassGenerator)

    def test_no_generators(self):
        """Test that a ClassGenerator without registered generators does nothing."""
        cg = ClassGenerator()
        spec = GroupSpec(doc='A test group spec with a data type', data_type_def='Baz')
        cls = cg.generate_class(data_type='Baz', spec=spec, parent_cls=Container, attr_names={}, type_map=TypeMap())
        self.assertEqual(cls.__mro__, (cls, Container, AbstractContainer, object))
        self.assertTrue(hasattr(cls, '__init__'))


class TestDynamicContainer(TestCase):

    def setUp(self):
        self.bar_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Bar',
            datasets=[
                DatasetSpec(
                    doc='a dataset',
                    dtype='int',
                    name='data',
                    attributes=[AttributeSpec(name='attr2', doc='an integer attribute', dtype='int')]
                )
            ],
            attributes=[AttributeSpec(name='attr1', doc='a string attribute', dtype='text')])
        specs = [self.bar_spec]
        containers = {'Bar': Bar}
        self.type_map = create_test_type_map(specs, containers)
        self.spec_catalog = self.type_map.namespace_catalog.get_namespace(CORE_NAMESPACE).catalog

    def test_dynamic_container_creation(self):
        baz_spec = GroupSpec('A test extension with no Container class',
                             data_type_def='Baz', data_type_inc=self.bar_spec,
                             attributes=[AttributeSpec('attr3', 'a float attribute', 'float'),
                                         AttributeSpec('attr4', 'another float attribute', 'float')])
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        cls = self.type_map.get_dt_container_cls('Baz', CORE_NAMESPACE)
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
        cls = self.type_map.get_dt_container_cls('Baz', CORE_NAMESPACE)
        inst = cls(attr4=10.)
        self.assertEqual(inst.name, 'bingo')

    def test_dynamic_container_creation_defaults(self):
        baz_spec = GroupSpec('A test extension with no Container class',
                             data_type_def='Baz', data_type_inc=self.bar_spec,
                             attributes=[AttributeSpec('attr3', 'a float attribute', 'float'),
                                         AttributeSpec('attr4', 'another float attribute', 'float')])
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        cls = self.type_map.get_dt_container_cls('Baz', CORE_NAMESPACE)
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
        cls = self.type_map.get_dt_container_cls('Baz', CORE_NAMESPACE)
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
        cls = self.type_map.get_dt_container_cls('Baz', CORE_NAMESPACE)

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
            cls = self.type_map.get_dt_container_cls('Baz', CORE_NAMESPACE)

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
        Baz2 = self.type_map.get_dt_container_cls('Baz2', CORE_NAMESPACE)
        Baz1 = self.type_map.get_dt_container_cls('Baz1', CORE_NAMESPACE)
        Baz1('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0,
             baz2=Baz2('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0))

        Bar = self.type_map.get_dt_container_cls('Bar', CORE_NAMESPACE)
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
        Baz1 = self.type_map.get_dt_container_cls('Baz1', CORE_NAMESPACE)
        Baz2 = self.type_map.get_dt_container_cls('Baz2', CORE_NAMESPACE)
        Baz1('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0,
             baz2=Baz2('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0))

        Bar = self.type_map.get_dt_container_cls('Bar', CORE_NAMESPACE)
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
            self.type_map.get_dt_container_cls('Baz1', CORE_NAMESPACE)

    def test_dynamic_container_fixed_name(self):
        """Test that dynamic class generation for an extended type with a fixed name works."""
        baz_spec = GroupSpec('A test extension with no Container class',
                             data_type_def='Baz', data_type_inc=self.bar_spec, name='Baz')
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        Baz = self.type_map.get_dt_container_cls('Baz', CORE_NAMESPACE)
        obj = Baz([1, 2, 3, 4], 'string attribute', attr2=1000)
        self.assertEqual(obj.name, 'Baz')

    def test_multi_container_spec(self):
        multi_spec = GroupSpec(
            doc='A test extension that contains a multi',
            data_type_def='Multi',
            groups=[
                GroupSpec(data_type_inc=self.bar_spec, doc='test multi', quantity='*')
            ],
            attributes=[
                AttributeSpec(name='attr3', doc='a float attribute', dtype='float')
            ]
        )
        self.spec_catalog.register_spec(multi_spec, 'extension.yaml')
        Bar = self.type_map.get_dt_container_cls('Bar', CORE_NAMESPACE)
        Multi = self.type_map.get_dt_container_cls('Multi', CORE_NAMESPACE)
        assert issubclass(Multi, MultiContainerInterface)
        assert Multi.__clsconf__ == [
            dict(
                attr='bars',
                type=Bar,
                add='add_bars',
                get='get_bars',
                create='create_bars'
            )
        ]

        multi = Multi(
            name='my_multi',
            bars=[Bar('my_bar', list(range(10)), 'value1', 10)],
            attr3=5.
        )
        assert multi.bars['my_bar'] == Bar('my_bar', list(range(10)), 'value1', 10)
        assert multi.attr3 == 5.


class TestGetClassSeparateNamespace(TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        if os.path.exists(self.test_dir):  # start clean
            self.tearDown()
        os.mkdir(self.test_dir)

        self.bar_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Bar',
            datasets=[
                DatasetSpec(name='data', doc='a dataset', dtype='int')
            ],
            attributes=[
                AttributeSpec(name='attr1', doc='a string attribute', dtype='text'),
                AttributeSpec(name='attr2', doc='an integer attribute', dtype='int')
            ]
        )
        self.type_map = TypeMap()
        create_load_namespace_yaml(
            namespace_name=CORE_NAMESPACE,
            specs=[self.bar_spec],
            output_dir=self.test_dir,
            incl_types=dict(),
            type_map=self.type_map
        )

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_get_class_separate_ns(self):
        """Test that get_class correctly sets the name and type hierarchy across namespaces."""
        self.type_map.register_container_type(CORE_NAMESPACE, 'Bar', Bar)
        baz_spec = GroupSpec(
            doc='A test extension',
            data_type_def='Baz',
            data_type_inc='Bar',
        )
        create_load_namespace_yaml(
            namespace_name='ndx-test',
            specs=[baz_spec],
            output_dir=self.test_dir,
            incl_types={CORE_NAMESPACE: ['Bar']},
            type_map=self.type_map
        )

        cls = self.type_map.get_dt_container_cls('Baz', 'ndx-test')
        self.assertEqual(cls.__name__, 'Baz')
        self.assertTrue(issubclass(cls, Bar))

    def _build_separate_namespaces(self):
        # create an empty extension to test ClassGenerator._get_container_type resolution
        # the Bar class has not been mapped yet to the bar spec
        qux_spec = DatasetSpec(
            doc='A test extension',
            data_type_def='Qux'
        )
        spam_spec = DatasetSpec(
            doc='A test extension',
            data_type_def='Spam'
        )
        create_load_namespace_yaml(
            namespace_name='ndx-qux',
            specs=[qux_spec, spam_spec],
            output_dir=self.test_dir,
            incl_types={},
            type_map=self.type_map
        )
        # resolve Spam first so that ndx-qux is resolved first
        self.type_map.get_dt_container_cls('Spam', 'ndx-qux')

        baz_spec = GroupSpec(
            doc='A test extension',
            data_type_def='Baz',
            data_type_inc='Bar',
            groups=[
                GroupSpec(data_type_inc='Qux', doc='a qux', quantity='?'),
                GroupSpec(data_type_inc='Bar', doc='a bar', quantity='?')
            ]
        )
        create_load_namespace_yaml(
            namespace_name='ndx-test',
            specs=[baz_spec],
            output_dir=self.test_dir,
            incl_types={
                CORE_NAMESPACE: ['Bar'],
                'ndx-qux': ['Qux']
            },
            type_map=self.type_map
        )

    def _check_classes(self, baz_cls, bar_cls, bar_cls2, qux_cls, qux_cls2):
        self.assertEqual(qux_cls.__name__, 'Qux')
        self.assertEqual(baz_cls.__name__, 'Baz')
        self.assertEqual(bar_cls.__name__, 'Bar')
        self.assertIs(bar_cls, bar_cls2)  # same class, two different namespaces
        self.assertIs(qux_cls, qux_cls2)
        self.assertTrue(issubclass(qux_cls, Data))
        self.assertTrue(issubclass(baz_cls, bar_cls))
        self.assertTrue(issubclass(bar_cls, Container))

        qux_inst = qux_cls(name='qux_name', data=[1])
        bar_inst = bar_cls(name='bar_name', data=100, attr1='a string', attr2=10)
        baz_inst = baz_cls(name='baz_name', qux=qux_inst, bar=bar_inst, data=100, attr1='a string', attr2=10)
        self.assertIs(baz_inst.qux, qux_inst)

    def test_get_class_include_from_separate_ns_1(self):
        """Test that get_class correctly sets the name and includes types correctly across namespaces.
        This is one of multiple tests carried out to ensure that order of which get_dt_container_cls is called
        does not impact the results

        first use EXTENSION namespace, then use ORIGINAL namespace
        """
        self._build_separate_namespaces()

        baz_cls = self.type_map.get_dt_container_cls('Baz', 'ndx-test')  # Qux and Bar are not yet resolved
        bar_cls = self.type_map.get_dt_container_cls('Bar', 'ndx-test')
        bar_cls2 = self.type_map.get_dt_container_cls('Bar', CORE_NAMESPACE)
        qux_cls = self.type_map.get_dt_container_cls('Qux', 'ndx-test')
        qux_cls2 = self.type_map.get_dt_container_cls('Qux', 'ndx-qux')

        self._check_classes(baz_cls, bar_cls, bar_cls2, qux_cls, qux_cls2)

    def test_get_class_include_from_separate_ns_2(self):
        """Test that get_class correctly sets the name and includes types correctly across namespaces.
        This is one of multiple tests carried out to ensure that order of which get_dt_container_cls is called
        does not impact the results

        first use ORIGINAL namespace, then use EXTENSION namespace
        """
        self._build_separate_namespaces()

        baz_cls = self.type_map.get_dt_container_cls('Baz', 'ndx-test')  # Qux and Bar are not yet resolved
        bar_cls2 = self.type_map.get_dt_container_cls('Bar', CORE_NAMESPACE)
        bar_cls = self.type_map.get_dt_container_cls('Bar', 'ndx-test')
        qux_cls = self.type_map.get_dt_container_cls('Qux', 'ndx-test')
        qux_cls2 = self.type_map.get_dt_container_cls('Qux', 'ndx-qux')

        self._check_classes(baz_cls, bar_cls, bar_cls2, qux_cls, qux_cls2)

    def test_get_class_include_from_separate_ns_3(self):
        """Test that get_class correctly sets the name and includes types correctly across namespaces.
        This is one of multiple tests carried out to ensure that order of which get_dt_container_cls is called
        does not impact the results

        first use EXTENSION namespace, then use EXTENSION namespace
        """
        self._build_separate_namespaces()

        baz_cls = self.type_map.get_dt_container_cls('Baz', 'ndx-test')  # Qux and Bar are not yet resolved
        bar_cls = self.type_map.get_dt_container_cls('Bar', 'ndx-test')
        bar_cls2 = self.type_map.get_dt_container_cls('Bar', CORE_NAMESPACE)
        qux_cls2 = self.type_map.get_dt_container_cls('Qux', 'ndx-qux')
        qux_cls = self.type_map.get_dt_container_cls('Qux', 'ndx-test')

        self._check_classes(baz_cls, bar_cls, bar_cls2, qux_cls, qux_cls2)

    def test_get_class_include_from_separate_ns_4(self):
        """Test that get_class correctly sets the name and includes types correctly across namespaces.
        This is one of multiple tests carried out to ensure that order of which get_dt_container_cls is called
        does not impact the results

        first use ORIGINAL namespace, then use EXTENSION namespace
        """
        self._build_separate_namespaces()

        baz_cls = self.type_map.get_dt_container_cls('Baz', 'ndx-test')  # Qux and Bar are not yet resolved
        bar_cls2 = self.type_map.get_dt_container_cls('Bar', CORE_NAMESPACE)
        bar_cls = self.type_map.get_dt_container_cls('Bar', 'ndx-test')
        qux_cls2 = self.type_map.get_dt_container_cls('Qux', 'ndx-qux')
        qux_cls = self.type_map.get_dt_container_cls('Qux', 'ndx-test')

        self._check_classes(baz_cls, bar_cls, bar_cls2, qux_cls, qux_cls2)


class EmptyBar(Container):
    pass


class TestBaseProcessFieldSpec(TestCase):

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

    def test_update_docval(self):
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
                    type_map=self.type_map,
                    spec=spec
                )
                self.assertListEqual(docval_args, expected[:(i+1)])  # compare with the first i elements of expected

    def test_update_docval_attr_shape(self):
        """Test that update_docval_args for an attribute with shape sets the type and shape keys."""
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
            type_map=TypeMap(),
            spec=spec
        )

        expected = [{'name': 'attr1', 'type': ('array_data', 'data'), 'doc': 'a string attribute', 'shape': [None]}]
        self.assertListEqual(docval_args, expected)

    def test_update_docval_dset_shape(self):
        """Test that update_docval_args for a dataset with shape sets the type and shape keys."""
        spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Baz',
            datasets=[
                DatasetSpec(name='dset1', doc='a string dataset', dtype='text', shape=[None])
            ]
        )
        not_inherited_fields = {'dset1': spec.get_dataset('dset1')}

        docval_args = list()
        CustomClassGenerator.process_field_spec(
            classdict={},
            docval_args=docval_args,
            parent_cls=EmptyBar,  # <-- arbitrary class
            attr_name='dset1',
            not_inherited_fields=not_inherited_fields,
            type_map=TypeMap(),
            spec=spec
        )

        expected = [{'name': 'dset1', 'type': ('array_data', 'data'), 'doc': 'a string dataset', 'shape': [None]}]
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
            type_map=TypeMap(),
            spec=spec
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
            type_map=TypeMap(),
            spec=spec
        )

        expected = [{'name': 'attr1', 'type': str, 'doc': 'a string attribute', 'default': None}]
        self.assertListEqual(docval_args, expected)

    def test_update_docval_default_value_none_required_parent(self):
        """Test that update_docval_args for an optional field with a required parent sets default: None."""
        spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Baz',
            groups=[
                GroupSpec(
                    name='group1',
                    doc='required untyped group',
                    attributes=[
                        AttributeSpec(name='attr1', doc='a string attribute', dtype='text', required=False)
                    ]
                )
            ]
        )
        not_inherited_fields = {'attr1': spec.get_group('group1').get_attribute('attr1')}

        docval_args = list()
        CustomClassGenerator.process_field_spec(
            classdict={},
            docval_args=docval_args,
            parent_cls=EmptyBar,  # <-- arbitrary class
            attr_name='attr1',
            not_inherited_fields=not_inherited_fields,
            type_map=TypeMap(),
            spec=spec
        )

        expected = [{'name': 'attr1', 'type': str, 'doc': 'a string attribute', 'default': None}]
        self.assertListEqual(docval_args, expected)

    def test_update_docval_required_field_optional_parent(self):
        """Test that update_docval_args for a required field with an optional parent sets default: None."""
        spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Baz',
            groups=[
                GroupSpec(
                    name='group1',
                    doc='required untyped group',
                    attributes=[
                        AttributeSpec(name='attr1', doc='a string attribute', dtype='text')
                    ],
                    quantity='?'
                )
            ]
        )
        not_inherited_fields = {'attr1': spec.get_group('group1').get_attribute('attr1')}

        docval_args = list()
        CustomClassGenerator.process_field_spec(
            classdict={},
            docval_args=docval_args,
            parent_cls=EmptyBar,  # <-- arbitrary class
            attr_name='attr1',
            not_inherited_fields=not_inherited_fields,
            type_map=TypeMap(),
            spec=spec
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
            type_map=TypeMap(),
            spec=spec
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
            type_map=self.type_map,
            spec=GroupSpec('dummy', 'doc')
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

        classdict = {}
        bases = [Container]
        docval_args = [{'name': 'name', 'type': str, 'doc': 'name'},
                       {'name': 'attr1', 'type': ('array_data', 'data'), 'doc': 'a string attribute',
                        'shape': [None]}]
        CustomClassGenerator.post_process(classdict, bases, docval_args, spec)

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

        classdict = {}
        bases = [Container]
        docval_args = [{'name': 'name', 'type': str, 'doc': 'name'},
                       {'name': 'attr1', 'type': ('array_data', 'data'), 'doc': 'a string attribute',
                        'shape': [None]}]
        CustomClassGenerator.post_process(classdict, bases, docval_args, spec)

        expected = [{'name': 'name', 'type': str, 'doc': 'name', 'default': 'MyBaz'},
                    {'name': 'attr1', 'type': ('array_data', 'data'), 'doc': 'a string attribute',
                     'shape': [None]}]
        self.assertListEqual(docval_args, expected)


class TestMCIProcessFieldSpec(TestCase):

    def setUp(self):
        bar_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='EmptyBar'
        )
        specs = [bar_spec]
        container_classes = {'EmptyBar': EmptyBar}
        self.type_map = create_test_type_map(specs, container_classes)

    def test_update_docval(self):
        spec = GroupSpec(data_type_inc='EmptyBar', doc='test multi', quantity='*')
        classdict = dict()
        docval_args = []
        not_inherited_fields = {'empty_bars': spec}
        MCIClassGenerator.process_field_spec(
            classdict=classdict,
            docval_args=docval_args,
            parent_cls=Container,
            attr_name='empty_bars',
            not_inherited_fields=not_inherited_fields,
            type_map=self.type_map,
            spec=spec
        )

        expected = [
            dict(
                attr='empty_bars',
                type=EmptyBar,
                add='add_empty_bars',
                get='get_empty_bars',
                create='create_empty_bars'
            )
        ]
        self.assertEqual(classdict['__clsconf__'], expected)

    def test_update_init_zero_or_more(self):
        spec = GroupSpec(data_type_inc='EmptyBar', doc='test multi', quantity='*')
        classdict = dict()
        docval_args = []
        not_inherited_fields = {'empty_bars': spec}
        MCIClassGenerator.process_field_spec(
            classdict=classdict,
            docval_args=docval_args,
            parent_cls=Container,
            attr_name='empty_bars',
            not_inherited_fields=not_inherited_fields,
            type_map=self.type_map,
            spec=spec
        )

        expected = [{'name': 'empty_bars', 'type': (list, tuple, dict, EmptyBar), 'doc': 'test multi', 'default': None}]
        self.assertListEqual(docval_args, expected)

    def test_update_init_one_or_more(self):
        spec = GroupSpec(data_type_inc='EmptyBar', doc='test multi', quantity='+')
        classdict = dict()
        docval_args = []
        not_inherited_fields = {'empty_bars': spec}
        MCIClassGenerator.process_field_spec(
            classdict=classdict,
            docval_args=docval_args,
            parent_cls=Container,
            attr_name='empty_bars',
            not_inherited_fields=not_inherited_fields,
            type_map=self.type_map,
            spec=spec
        )

        expected = [{'name': 'empty_bars', 'type': (list, tuple, dict, EmptyBar), 'doc': 'test multi'}]
        self.assertListEqual(docval_args, expected)

    def test_post_process(self):
        multi_spec = GroupSpec(
            doc='A test extension that contains a multi',
            data_type_def='Multi',
            groups=[
                GroupSpec(data_type_inc='EmptyBar', doc='test multi', quantity='*')
            ],
        )
        classdict = dict(
            __clsconf__=[
                dict(
                    attr='empty_bars',
                    type=EmptyBar,
                    add='add_empty_bars',
                    get='get_empty_bars',
                    create='create_empty_bars'
                )
            ]
        )
        bases = [Container]
        docval_args = []
        MCIClassGenerator.post_process(classdict, bases, docval_args, multi_spec)
        self.assertEqual(bases, [MultiContainerInterface, Container])

    def test_post_process_already_multi(self):
        class Multi1(MultiContainerInterface):
            pass

        multi_spec = GroupSpec(
            doc='A test extension that contains a multi and extends a multi',
            data_type_def='Multi2',
            data_type_inc='Multi1',
            groups=[
                GroupSpec(data_type_inc='EmptyBar', doc='test multi', quantity='*')
            ],
        )
        classdict = dict(
            __clsconf__=[
                dict(
                    attr='empty_bars',
                    type=EmptyBar,
                    add='add_empty_bars',
                    get='get_empty_bars',
                    create='create_empty_bars'
                )
            ]
        )
        bases = [Multi1]
        docval_args = []
        MCIClassGenerator.post_process(classdict, bases, docval_args, multi_spec)
        self.assertEqual(bases, [Multi1])
