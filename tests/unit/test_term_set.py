import os
import numpy as np

from hdmf import Container
from hdmf.term_set import TermSet, TermSetWrapper, TypeConfigurator
from hdmf.testing import TestCase, remove_test_file
from hdmf.common import (VectorData, unload_type_config,
                         get_loaded_type_config, load_type_config)
from hdmf.utils import popargs


CUR_DIR = os.path.dirname(os.path.realpath(__file__))

try:
    from linkml_runtime.utils.schemaview import SchemaView  # noqa: F401
    import schemasheets  # noqa: F401
    import oaklib  # noqa: F401
    import yaml  # noqa: F401

    REQUIREMENTS_INSTALLED = True
except ImportError:
    REQUIREMENTS_INSTALLED = False

class TestTermSet(TestCase):
    """Tests for TermSet"""
    def setUp(self):
        if not REQUIREMENTS_INSTALLED:
            self.skipTest("optional LinkML module is not installed")

    def test_termset_setup(self):
        termset = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        self.assertEqual(termset.name, 'Species')
        self.assertEqual(list(termset.sources), ['NCBI_TAXON'])

    def test_repr_short(self):
        termset = TermSet(term_schema_path='tests/unit/example_test_term_set2.yaml')
        output = ('Schema Path: tests/unit/example_test_term_set2.yaml\nSources: NCBI_TAXON\nTerms: \n'
                  '   - Homo sapiens\n   - Mus musculus\n   - Ursus arctos horribilis\nNumber of terms: 3')
        self.assertEqual(repr(termset), output)

    def test_repr_html_short(self):
        termset = TermSet(term_schema_path='tests/unit/example_test_term_set2.yaml')
        output = ('<b>Schema Path: </b>tests/unit/example_test_term_set2.yaml<br><b>Sources:'
                  ' </b>NCBI_TAXON<br><b> Terms: </b><li> Homo sapiens </li><li> Mus musculus'
                  ' </li><li> Ursus arctos horribilis </li><i> Number of terms:</i> 3')
        self.assertEqual(termset._repr_html_(), output)

    def test_repr_long(self):
        termset = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        output = ('Schema Path: tests/unit/example_test_term_set.yaml\nSources: NCBI_TAXON\nTerms: \n'
                  '   - Homo sapiens\n   - Mus musculus\n   - Ursus arctos horribilis\n   ... ... \n'
                  '   - Ailuropoda melanoleuca\nNumber of terms: 5')
        self.assertEqual(repr(termset), output)

    def test_repr_html_long(self):
        termset = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        output = ('<b>Schema Path: </b>tests/unit/example_test_term_set.yaml<br><b>Sources:'
                  ' </b>NCBI_TAXON<br><b> Terms: </b><li> Homo sapiens </li><li> Mus musculus'
                  ' </li><li> Ursus arctos horribilis </li>... ...<li> Ailuropoda melanoleuca'
                  ' </li><i> Number of terms:</i> 5')
        self.assertEqual(termset._repr_html_(), output)

    def test_view_set(self):
        termset = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        expected = ['Homo sapiens', 'Mus musculus', 'Ursus arctos horribilis', 'Myrmecophaga tridactyla',
                    'Ailuropoda melanoleuca']
        self.assertEqual(list(termset.view_set), expected)
        self.assertIsInstance(termset.view, SchemaView)

    def test_termset_validate(self):
        termset = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        self.assertEqual(termset.validate('Homo sapiens'), True)

    def test_termset_validate_false(self):
        termset = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        self.assertEqual(termset.validate('missing_term'), False)

    def test_get_item(self):
        termset = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        self.assertEqual(termset['Homo sapiens'].id, 'NCBI_TAXON:9606')
        self.assertEqual(termset['Homo sapiens'].description, 'the species is human')
        self.assertEqual(
            termset['Homo sapiens'].meaning,
            'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606'
        )

    def test_get_item_key_error(self):
        termset = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        with self.assertRaises(ValueError):
            termset['Homo Ssapiens']

    def test_schema_sheets_and_path_provided_error(self):
        folder = os.path.join(CUR_DIR, "test_term_set_input", "schemasheets")
        with self.assertRaises(ValueError):
            TermSet(term_schema_path='tests/unit/example_test_term_set.yaml', schemasheets_folder=folder)

    def test_view_set_sheets(self):
        folder = os.path.join(CUR_DIR, "test_term_set_input", "schemasheets")
        termset = TermSet(schemasheets_folder=folder)
        expected = ['ASTROCYTE', 'INTERNEURON', 'MICROGLIAL_CELL', 'MOTOR_NEURON',
                    'OLIGODENDROCYTE', 'PYRAMIDAL_NEURON']
        self.assertEqual(list(termset.view_set), expected)
        self.assertIsInstance(termset.view, SchemaView)

    def test_enum_expander(self):
        schema_path = 'tests/unit/example_dynamic_term_set.yaml'
        termset = TermSet(term_schema_path=schema_path, dynamic=True)
        # check that interneuron term is in materialized schema
        self.assertIn("CL:0000099", termset.view_set)
        # check that motor neuron term is in materialized schema
        self.assertIn("CL:0000100", termset.view_set)
        # check that pyramidal neuron is in materialized schema
        self.assertIn("CL:0000598", termset.view_set)

        self.assertIsInstance(termset.view, SchemaView)
        expected_path = os.path.join("tests", "unit", "expanded_example_dynamic_term_set.yaml")
        expected_path = os.path.normpath(expected_path)
        actual_path = os.path.normpath(termset.expanded_termset_path)

        self.assertEqual(actual_path, expected_path)

        filename = os.path.splitext(os.path.basename(schema_path))[0]
        remove_test_file(f"tests/unit/expanded_{filename}.yaml")

    def test_enum_expander_output(self):
        schema_path = 'tests/unit/example_dynamic_term_set.yaml'
        termset = TermSet(term_schema_path=schema_path, dynamic=True)
        convert_path = termset._TermSet__enum_expander()
        convert_path = os.path.normpath(convert_path)

        expected_path = os.path.join("tests", "unit", "expanded_example_dynamic_term_set.yaml")
        expected_path = os.path.normpath(expected_path)

        self.assertEqual(convert_path, expected_path)

        filename = os.path.splitext(os.path.basename(schema_path))[0]
        remove_test_file(f"tests/unit/expanded_{filename}.yaml")

    def test_folder_output(self):
        folder = os.path.join(CUR_DIR, "test_term_set_input", "schemasheets")
        termset = TermSet(schemasheets_folder=folder)
        actual_path = termset._TermSet__schemasheets_convert()
        expected_path = os.path.normpath(os.path.join(os.path.dirname(folder), "schemasheets/nwb_static_enums.yaml"))
        self.assertEqual(actual_path, expected_path)


class TestTermSetWrapper(TestCase):
    """Tests for the TermSetWrapper"""
    def setUp(self):
        if not REQUIREMENTS_INSTALLED:
            self.skipTest("optional LinkML module is not installed")

        self.termset = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')

        self.wrapped_array = TermSetWrapper(value=np.array(['Homo sapiens']), termset=self.termset)
        self.wrapped_list = TermSetWrapper(value=['Homo sapiens'], termset=self.termset)

        c_data = np.array([('Homo sapiens', 24)], dtype=[('species', 'U50'), ('age', 'i4')])
        self.wrapped_comp_array = TermSetWrapper(value=c_data,
                                                 termset=self.termset,
                                                 field='species')

        self.np_data = VectorData(
            name='Species_1',
            description='...',
            data=self.wrapped_array
        )

    def test_properties(self):
        self.assertEqual(self.wrapped_array.value, ['Homo sapiens'])
        self.assertEqual(self.wrapped_array.termset.view_set, self.termset.view_set)
        self.assertEqual(self.wrapped_array.dtype, 'U12') # this covers __getattr__
        self.assertEqual(self.wrapped_comp_array.field, 'species')

    def test_get_item(self):
        self.assertEqual(self.np_data.data[0], 'Homo sapiens')

    def test_validate_error(self):
        with self.assertRaises(ValueError):
            VectorData(name='Species_1',
                       description='...',
                       data=TermSetWrapper(value=['Missing Term'],
                       termset=self.termset))

    def test_wrapper_validate_attribute(self):
        col1 = VectorData(
            name='Species_1',
            description=TermSetWrapper(value='Homo sapiens',
                                       termset=self.termset),
            data=['Human']
        )
        self.assertTrue(isinstance(col1.description, TermSetWrapper))

    def test_wrapper_validate_dataset(self):
        col1 = VectorData(
            name='Species_1',
            description='...',
            data=TermSetWrapper(value=['Homo sapiens'],
                                termset=self.termset)
        )
        self.assertTrue(isinstance(col1.data, TermSetWrapper))

    def test_wrapper_append(self):
        data_obj = VectorData(name='species', description='...', data=self.wrapped_list)
        data_obj.append('Mus musculus')
        self.assertEqual(data_obj.data.value, ['Homo sapiens', 'Mus musculus'])

    def test_wrapper_append_error(self):
        data_obj = VectorData(name='species', description='...', data=self.wrapped_list)
        with self.assertRaises(ValueError):
            data_obj.append('bad_data')

    def test_wrapper_extend(self):
        data_obj = VectorData(name='species', description='...', data=self.wrapped_list)
        data_obj.extend(['Mus musculus'])
        self.assertEqual(data_obj.data.value, ['Homo sapiens', 'Mus musculus'])

    def test_wrapper_extend_error(self):
        data_obj = VectorData(name='species', description='...', data=self.wrapped_list)
        with self.assertRaises(ValueError):
            data_obj.extend(['bad_data'])

class TestTypeConfig(TestCase):
    def setUp(self):
        if not REQUIREMENTS_INSTALLED:
            self.skipTest("optional LinkML module is not installed")

    def tearDown(self):
        unload_type_config()

    def test_get_loaded_type_config_error(self):
        with self.assertRaises(ValueError):
            get_loaded_type_config()

    def test_config_path(self):
        path = 'tests/unit/hdmf_config.yaml'
        tc = TypeConfigurator(path=path)
        self.assertEqual(tc.path, [path])

    def test_get_config(self):
        path = 'tests/unit/hdmf_config.yaml'
        tc = TypeConfigurator(path=path)
        self.assertEqual(tc.get_config('VectorData', 'hdmf-common'),
                                      {'description': {'termset': 'example_test_term_set.yaml'}})

    def test_get_config_namespace_error(self):
        path = 'tests/unit/hdmf_config.yaml'
        tc = TypeConfigurator(path=path)
        with self.assertRaises(ValueError):
            tc.get_config('VectorData', 'hdmf-common11')

    def test_get_config_container_error(self):
        path = 'tests/unit/hdmf_config.yaml'
        tc = TypeConfigurator(path=path)
        with self.assertRaises(ValueError):
            tc.get_config('VectorData11', 'hdmf-common')

    def test_already_loaded_path_error(self):
        path = 'tests/unit/hdmf_config.yaml'
        tc = TypeConfigurator(path=path)
        with self.assertRaises(ValueError):
            tc.load_type_config(config_path=path)

    def test_load_two_unique_configs(self):
        path = 'tests/unit/hdmf_config.yaml'
        path2 = 'tests/unit/hdmf_config2.yaml'
        tc = TypeConfigurator(path=path)
        tc.load_type_config(config_path=path2)
        config = {'namespaces': {'hdmf-common': {'version': '3.12.2',
                  'data_types': {'VectorData': {'name': None},
                  'VectorIndex': {'data': '...'},
                  'Data': {'description': {'termset': 'example_test_term_set.yaml'}},
                  'EnumData': {'description': {'termset': 'example_test_term_set.yaml'}}}},
                  'foo_namespace': {'version': '...',
                  'data_types': {'ExtensionContainer': {'description': None}}},
                  'namespace2': {'version': 0, 'data_types':
                  {'MythicData': {'description':
                  {'termset': 'example_test_term_set.yaml'}}}}}}
        self.assertEqual(tc.path, [path, path2])
        self.assertEqual(tc.config, config)


class ExtensionContainer(Container):
    __fields__ = ("description",)

    def __init__(self, **kwargs):
        description, namespace = popargs('description', 'namespace', kwargs)
        self.namespace = namespace
        super().__init__(**kwargs)
        self.description = description

    @property
    def data_type(self):
        """
        Return the spec data type associated with this container.
        """
        return "ExtensionContainer"


class TestGlobalTypeConfig(TestCase):
    def setUp(self):
        if not REQUIREMENTS_INSTALLED:
            self.skipTest("optional LinkML module is not installed")
        load_type_config(config_path='tests/unit/hdmf_config.yaml')

    def tearDown(self):
        unload_type_config()

    def test_load_config(self):
        config = get_loaded_type_config()
        self.assertEqual(config,
        {'namespaces': {'hdmf-common': {'version': '3.12.2',
         'data_types': {'VectorData':
        {'description': {'termset': 'example_test_term_set.yaml'}},
         'VectorIndex': {'data': '...'}}}, 'foo_namespace':
        {'version': '...', 'data_types':
        {'ExtensionContainer': {'description': None}}}}}
)

    def test_validate_with_config(self):
        data = VectorData(name='foo', data=[0], description='Homo sapiens')
        self.assertEqual(data.description.value, 'Homo sapiens')

    def test_namespace_warn(self):
        with self.assertWarns(Warning):
            ExtensionContainer(name='foo',
                               namespace='foo',
                               description='Homo sapiens')

    def test_container_type_warn(self):
        with self.assertWarns(Warning):
            ExtensionContainer(name='foo',
                               namespace='hdmf-common',
                               description='Homo sapiens')

    def test_already_wrapped_warn(self):
        terms = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        with self.assertWarns(Warning):
            VectorData(name='foo',
                       data=[0],
                       description=TermSetWrapper(value='Homo sapiens', termset=terms))

    def test_field_not_in_config(self):
        unload_type_config()
        load_type_config(config_path='tests/unit/hdmf_config2.yaml')

        VectorData(name='foo', data=[0], description='Homo sapiens')

    def test_spec_none(self):
        with self.assertWarns(Warning):
            ExtensionContainer(name='foo',
                               namespace='foo_namespace',
                               description='Homo sapiens')
