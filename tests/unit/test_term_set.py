from hdmf.term_set import TermSet
from hdmf.testing import TestCase
import unittest


try:
    import linkml_runtime  # noqa: F401
    LINKML_INSTALLED = True
except ImportError:
    LINKML_INSTALLED = False

class TestTermSet(TestCase):

    @unittest.skipIf(not LINKML_INSTALLED, "optional LinkML module is not installed")
    def test_termset_setup(self):
        termset = TermSet(name='species', term_schema_path='tests/unit/example_test_term_set.yaml')
        self.assertEqual(list(termset.sources), ['NCBI_TAXON', 'Ensemble'])

    @unittest.skipIf(not LINKML_INSTALLED, "optional LinkML module is not installed")
    def test_view_set(self):
        termset = TermSet(name='species', term_schema_path='tests/unit/example_test_term_set.yaml')
        expected = ['Homo sapiens', 'Mus musculus', 'Ursus arctos horribilis', 'Myrmecophaga tridactyla']
        self.assertEqual(list(termset.view_set), expected)

    @unittest.skipIf(not LINKML_INSTALLED, "optional LinkML module is not installed")
    def test_get_item(self):
        termset = TermSet(name='species', term_schema_path='tests/unit/example_test_term_set.yaml')
        self.assertEqual(termset['Homo sapiens'].id, 'NCBI_TAXON:9606')
        self.assertEqual(termset['Homo sapiens'].description, 'description')
        self.assertEqual(termset['Homo sapiens'].meaning, 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606')

    @unittest.skipIf(not LINKML_INSTALLED, "optional LinkML module is not installed")
    def test_get_item_key_error(self):
        termset = TermSet(name='species', term_schema_path='tests/unit/example_test_term_set.yaml')
        with self.assertRaises(ValueError):
            termset['Homo Ssapiens']
