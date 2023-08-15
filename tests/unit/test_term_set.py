import os

from hdmf.term_set import TermSet
from hdmf.testing import TestCase, remove_test_file


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

    def setUp(self):
        if not REQUIREMENTS_INSTALLED:
            self.skipTest("optional LinkML module is not installed")

    def test_termset_setup(self):
        termset = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        self.assertEqual(list(termset.sources), ['NCBI_TAXON'])

    def test_view_set(self):
        termset = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        expected = ['Homo sapiens', 'Mus musculus', 'Ursus arctos horribilis', 'Myrmecophaga tridactyla']
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
        self.assertEqual(termset['Homo sapiens'].meaning, 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606')

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
        actual_path = os.path.normpath(termset.expanded_term_set_path)

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

    def test_folder_output(self):
        folder = os.path.join(CUR_DIR, "test_term_set_input", "schemasheets")
        termset = TermSet(schemasheets_folder=folder)
        actual_path = termset._TermSet__schemasheets_convert()
        expected_path = os.path.normpath(os.path.join(os.path.dirname(folder), "schemasheets/nwb_static_enums.yaml"))
        self.assertEqual(actual_path, expected_path)
