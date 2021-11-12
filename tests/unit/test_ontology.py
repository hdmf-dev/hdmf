from hdmf import Ontology, EnsemblOntology, WebAPIOntology, LocalOntology, NCBI_Taxonomy
from hdmf.testing import TestCase

class TestOntology(TestCase):

    def test_constructor(self):
        ontology = Ontology(version='1.0', ontology_name='ontology_name', ontology_uri='ontology_uri')

        self.assertEqual(ontology.version, '1.0')
        self.assertEqual(ontology.ontology_name, 'ontology_name')
        self.assertEqual(ontology.ontology_uri, 'ontology_uri')


class TestWebAPIOntology(TestCase):

    def test_get_entity_browser(self):
        ontology = WebAPIOntology(version='1.0', ontology_name='Ensembl', ontology_uri='https://rest.ensembl.org', extension='/taxonomy/id/')

        self.assertEqual(ontology.get_ontology_entity(key='Homo sapiens'), ('9606', 'https://rest.ensembl.org/taxonomy/id/Homo sapiens'))

    def test_bad_entity_browser_request(self):
        ontology = WebAPIOntology(version='1.0', ontology_name='Ensembl', ontology_uri='https://rest.ensembl.org', extension='/taxonomy/id/')
        with self.assertRaises(ValueError):
            ontology.get_ontology_entity(key='Invalid Key')

class TestLocalOntology(TestCase):
    ontology_entities={"Homo sapiens": ['9606', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606']}

    def test_constructor(self):
        ontology = LocalOntology(version='1.0', ontology_name='ontology_name', ontology_uri='ontology_uri', ontology_entities=TestLocalOntology.ontology_entities)

        self.assertEqual(ontology.version, '1.0')
        self.assertEqual(ontology.ontology_name, 'ontology_name')
        self.assertEqual(ontology.ontology_uri, 'ontology_uri')
        self.assertEqual(ontology.ontology_entities, TestLocalOntology.ontology_entities)

    def test_add_ontology_entity(self):
        ontology = LocalOntology(version='1.0', ontology_name='ontology_name', ontology_uri='ontology_uri')
        ontology.add_ontology_entity(key='Homo sapiens', entity_value=['9606', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606'])

        self.assertEqual(ontology.ontology_entities, TestLocalOntology.ontology_entities)

    def test_remove_ontology_entity(self):
        ontology = LocalOntology(version='1.0', ontology_name='ontology_name', ontology_uri='ontology_uri', ontology_entities=TestLocalOntology.ontology_entities)
        ontology.remove_ontology_entity('Homo sapiens')

        self.assertEqual(ontology.ontology_entities, {})

    def test_get_ontology_entity(self):
        ontology = LocalOntology(version='1.0', ontology_name='ontology_name', ontology_uri='ontology_uri', ontology_entities=TestLocalOntology.ontology_entities)

        self.assertEqual(ontology.get_ontology_entity('Homo sapiens'), ('9606', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606'))


class TestEnsemblOntology(TestCase):

    def test_constructor(self):
        ontology = EnsemblOntology(version='1.0')

        self.assertEqual(ontology.version, '1.0')
        self.assertEqual(ontology.ontology_uri, 'https://rest.ensembl.org')
        self.assertEqual(ontology.extension, '/taxonomy/id/')

    def test_get_entity_browser(self):
        ontology = EnsemblOntology(version='1.0')
        self.assertEqual(ontology.get_ontology_entity(key='Homo sapiens'), ('9606', 'https://rest.ensembl.org/taxonomy/id/Homo sapiens'))

    def test_bad_entity_browser_request(self):
        ontology = EnsemblOntology(version='1.0')
        with self.assertRaises(ValueError):
            ontology.get_ontology_entity(key='Invalid Key')


class TestNCBITaxonomy(TestCase):

    ontology_entities={"Homo sapiens": ['9606', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606']}

    def test_constructor(self):
        ontology = NCBI_Taxonomy(version='1.0')

        self.assertEqual(ontology.version, '1.0')
        self.assertEqual(ontology.ontology_name, 'NCBI_Taxonomy')
        self.assertEqual(ontology.ontology_uri, 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi')
        self.assertEqual(ontology.ontology_entities, TestNCBITaxonomy.ontology_entities)
