from hdmf import Ontology, EnsemblOntology, WebAPIOntology, LocalOntology, NCBI_Taxonomy, WebAPIOntologyException, LocalOntologyException
from hdmf.testing import TestCase
from copy import copy
from tests.unit.utils import get_temp_filepath
import os

class TestOntology(TestCase):

    def test_constructor(self):
        ontology = Ontology(version='1.0', ontology_name='ontology_name', ontology_uri='ontology_uri')

        self.assertEqual(ontology.version, '1.0')
        self.assertEqual(ontology.ontology_name, 'ontology_name')
        self.assertEqual(ontology.ontology_uri, 'ontology_uri')


class TestWebAPIOntology(TestCase):
    ontology_entities={"Homo sapiens": ['9606', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606']}

    def test_get_entity_local(self):
        ontology = WebAPIOntology(version='1.0', ontology_name='Ensembl', ontology_uri='https://rest.ensembl.org', extension='/taxonomy/id/', ontology_entities=TestLocalOntology.ontology_entities)

        self.assertEqual(ontology.get_ontology_entity(key='Homo sapiens'), ('9606', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606'))

    def test_get_entity_browser(self):
        ontology = WebAPIOntology(version='1.0', ontology_name='Ensembl', ontology_uri='https://rest.ensembl.org', extension='/taxonomy/id/')

        self.assertEqual(ontology.get_ontology_entity(key='Homo sapiens'), ('9606', 'https://rest.ensembl.org/taxonomy/id/Homo sapiens'))

    def test_bad_entity_browser_request(self):
        ontology = WebAPIOntology(version='1.0', ontology_name='Ensembl', ontology_uri='https://rest.ensembl.org', extension='/taxonomy/id/')
        with self.assertRaises(WebAPIOntologyException):
            ontology.get_ontology_entity(key='Invalid Key')

    def test_add_to_ontology_entity_browser_request(self):
        ontology = WebAPIOntology(version='1.0', ontology_name='Ensembl', ontology_uri='https://rest.ensembl.org', extension='/taxonomy/id/')
        entities = ontology.get_ontology_entity(key='Homo sapiens', add_entity=True)

        self.assertEqual(entities, ontology.ontology_entities)
        self.assertEqual(entities, {'Homo sapiens': ['9606', 'https://rest.ensembl.org/taxonomy/id/Homo sapiens']})

class TestLocalOntology(TestCase):
    ontology_entities={"Homo sapiens": ['9606', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606']}

    def setUp(self):
        self.path = get_temp_filepath()

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_constructor(self):
        ontology = LocalOntology(version='1.0', ontology_name='ontology_name', ontology_uri='ontology_uri', ontology_entities=TestLocalOntology.ontology_entities)

        self.assertEqual(ontology.version, '1.0')
        self.assertEqual(ontology.ontology_name, 'ontology_name')
        self.assertEqual(ontology.ontology_uri, 'ontology_uri')
        self.assertEqual(ontology.ontology_entities, {"Homo sapiens": ['9606', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606']})

    def test_add_ontology_entity(self):
        ontology = LocalOntology(version='1.0', ontology_name='ontology_name', ontology_uri='ontology_uri')
        ontology.add_ontology_entity(key='Homo sapiens', entity_value=['9606', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606'])

        self.assertEqual(ontology.ontology_entities, TestLocalOntology.ontology_entities)

    def test_write_ontology_yaml(self):
        ontology = LocalOntology(version='1.0', ontology_name='ontology_name', ontology_uri='ontology_uri', ontology_entities=TestLocalOntology.ontology_entities)
        self.assertEqual(ontology.ontology_entities, {"Homo sapiens": ['9606', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606']})

        ontology.write_ontology_yaml(path=self.path)
        contents = open(self.path).read()
        ontology_dict = ontology.read_ontology_yaml(self.path)

        self.assertEqual(ontology_dict, {"Homo sapiens": ['9606', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606']})
        self.assertEqual(contents, "Homo sapiens: ['9606', https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606]\n")

    def test_add_bad_arg_ontology_entity(self):
        ontology = LocalOntology(version='1.0', ontology_name='ontology_name', ontology_uri='ontology_uri')
        with self.assertRaises(ValueError):
            ontology.add_ontology_entity(key='Homo sapiens', entity_value=['https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606'])

        with self.assertRaises(ValueError):
            ontology.add_ontology_entity(key='Homo sapiens', entity_value=['9606', 'ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606'])

        with self.assertRaises(ValueError):
            ontology.add_ontology_entity(key='Homo sapiens', entity_value=['', 'ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606'])

    def test_remove_ontology_entity(self):
        ontology_dict = copy(TestLocalOntology.ontology_entities)
        ontology = LocalOntology(version='1.0', ontology_name='ontology_name', ontology_uri='ontology_uri', ontology_entities=ontology_dict)
        ontology.remove_ontology_entity('Homo sapiens')

        self.assertEqual(ontology.ontology_entities, {})

    def test_get_ontology_entity(self):
        ontology = LocalOntology(version='1.0', ontology_name='ontology_name', ontology_uri='ontology_uri', ontology_entities=TestLocalOntology.ontology_entities)

        self.assertEqual(ontology.get_ontology_entity('Homo sapiens'), ('9606', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606'))

    def test_get_ontology_entity_bad_arg(self):
        ontology = LocalOntology(version='1.0', ontology_name='ontology_name', ontology_uri='ontology_uri', ontology_entities=TestLocalOntology.ontology_entities)
        with self.assertRaises(LocalOntologyException):
            ontology.get_ontology_entity(key='Invalid Key')


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
        with self.assertRaises(WebAPIOntologyException):
            ontology.get_ontology_entity(key='Invalid Key')


class TestNCBITaxonomy(TestCase):

    ontology_entities={"Homo sapiens": ['9606', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606']}

    def test_constructor(self):
        ontology = NCBI_Taxonomy(version='1.0')

        self.assertEqual(ontology.version, '1.0')
        self.assertEqual(ontology.ontology_name, 'NCBI_Taxonomy')
        self.assertEqual(ontology.ontology_uri, 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi')
        self.assertEqual(ontology.ontology_entities, TestNCBITaxonomy.ontology_entities)
