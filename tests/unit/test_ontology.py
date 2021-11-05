from hdmf import Ontology, EnsemblOntology, BrowserOntology
from hdmf.testing import TestCase
import numpy as np

class TestOntology(TestCase):

    def test_constructor(self):
        ontology = Ontology(name='example', ontology_name='ontology_name', ontology_uri='ontology_uri')

        self.assertEqual(ontology.name, 'example')
        self.assertEqual(ontology.ontology_name, 'ontology_name')
        self.assertEqual(ontology.ontology_uri, 'ontology_uri')


class TestBrowserOntology(TestCase):

    def test_get_entity_browser(self):
        ontology = BrowserOntology(name='example', ontology_name='Ensembl', ontology_uri='https://rest.ensembl.org', extension='/taxonomy/id/')

        self.assertEqual(ontology.get_entity_browser(key='Homo sapiens'), ('9606', 'https://rest.ensembl.org/taxonomy/id/Homo sapiens'))

    def test_bad_entity_browser_request(self):
        ontology = BrowserOntology(name='example', ontology_name='Ensembl', ontology_uri='https://rest.ensembl.org', extension='/taxonomy/id/')
        with self.assertRaises(ValueError):
            ontology.get_entity_browser(key='Invalid Key')

class TestEnsemblOntology(TestCase):

    def test_constructor(self):
        ontology = EnsemblOntology(name='example')

        self.assertEqual(ontology.name, 'example')
        self.assertEqual(ontology.ontology_uri, 'https://rest.ensembl.org')
        self.assertEqual(ontology.extension, '/taxonomy/id/')

    def test_get_entity_browser(self):
        ontology = EnsemblOntology(name='example')
        self.assertEqual(ontology.get_entity_browser(key='Homo sapiens'), ('9606', 'https://rest.ensembl.org/taxonomy/id/Homo sapiens'))

    def test_bad_entity_browser_request(self):
        ontology = EnsemblOntology(name='example')
        with self.assertRaises(ValueError):
            ontology.get_entity_browser(key='Invalid Key')
