import os

from hdmf.term_set import TermSet, TermSetWrapper
from hdmf.testing import TestCase, remove_test_file
from hdmf.common import VectorData
import numpy as np


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

        self.np_data = VectorData(
            name='Species_1',
            description='...',
            data=self.wrapped_array
        )
        self.list_data = VectorData(
            name='Species_1',
            description='...',
            data=self.wrapped_list
        )

    def test_properties(self):
        self.assertEqual(self.wrapped_array.value, ['Homo sapiens'])
        self.assertEqual(self.wrapped_array.termset.view_set, self.termset.view_set)
        self.assertEqual(self.wrapped_array.dtype, 'U12') # this covers __getattr__

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
