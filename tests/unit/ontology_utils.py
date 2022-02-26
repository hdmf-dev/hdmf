"""
Utilities for testing with ontologies, used to avoid a strict dependency on PyNERT for testing

The module attempts to import the dependencies from PyNERT directly if available and otherwise
defines its own local skeleton classes for a few ontologies classed used for testing.
"""
try:
    from pynert.ontology import OntologyEntity, WebAPIOntology, LocalOntology
    from pynert.resources import EnsemblOntology, NCBI_Taxonomy
except ImportError:
    from dataclasses import dataclass
    from hdmf.ontology import Ontology, WebAPIOntologyException, LocalOntologyException

    @dataclass
    class OntologyEntity:
        entity_id: str
        entity_uri: str

        def __iter__(self):
            return iter((self.entity_id, self.entity_uri))

    class LocalOntology(Ontology):
        def __init__(self, version,  ontology_name, ontology_uri, ontology_entities=None):
            super().__init__(version, ontology_name, ontology_uri)
            self.ontology_entities = {} if ontology_entities is None else ontology_entities

        def get_ontology_entity(self, key):
            try:
                return self.ontology_entities[key]
            except KeyError:
                raise LocalOntologyException

    class WebAPIOntology(LocalOntology):
        def __init__(self, version,  ontology_name, ontology_uri, extension, ontology_entities=None):
            super().__init__(version, ontology_name, ontology_uri, ontology_entities)
            self.extension = extension

        def get_ontology_entity(self, key):
            try:
                return self.ontology_entities[key]
            except (KeyError, LocalOntologyException):
                raise WebAPIOntologyException

    class NCBI_Taxonomy(LocalOntology):
        def __init__(self, version):
            entities = {
                "Homo sapiens":
                    OntologyEntity(
                        entity_id='9606',
                        entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606')
            }
            super().__init__(version=version,
                             ontology_name='NCBI_Taxonomy',
                             ontology_uri="https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi",
                             ontology_entities=entities)

    class EnsemblOntology(WebAPIOntology):
        def __init__(self, version):
            entities = {
                "Homo sapiens":
                    OntologyEntity(
                        entity_id='9606',
                        entity_uri='https://rest.ensembl.org/taxonomy/id/Homo sapiens')
            }
            super().__init__(version=version,
                             ontology_name="Ensembl",
                             ontology_uri="https://rest.ensembl.org",
                             extension="/taxonomy/id/",
                             ontology_entities=entities)
