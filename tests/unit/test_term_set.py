from hdmf.term_set import TermSet
from hdmf.testing import TestCase

class TestTermSet(TestCase):

    def test_termset_setup(self):
        termset = TermSet(name='species', term_schema_path='./example_test_term_set.yaml')
        self.assertEqual(list(termset.sources), ['NCBI_TAXON', 'Ensemble'])

    def test_view_set(self):
        termset = TermSet(name='species', term_schema_path='./example_test_term_set.yaml')
        self.assertEqual(list(termset.view_set), ['Homo sapiens', 'Mus musculus', 'Ursus arctos horribilis', 'Myrmecophaga tridactyla'])

    def test_get_item(self):
        termset = TermSet(name='species', term_schema_path='./example_test_term_set.yaml')
        self.assertEqual(termset['Homo sapiens'].id, 'NCBI_TAXON:9606')
        self.assertEqual(termset['Homo sapiens'].description, 'description')
        self.assertEqual(termset['Homo sapiens'].meaning, 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606')

    def test_get_item_key_error(self):
        termset = TermSet(name='species', term_schema_path='./example_test_term_set.yaml')
        with self.assertRaises(ValueError):
            termset['Homo Ssapiens']
