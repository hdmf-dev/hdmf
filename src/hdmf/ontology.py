"""Simple module use to define class stubs to allow us to make pynerd an optional dependency"""
try:
    from pynert.ontology import Ontology
    from pynert.errors import OntologyEntityException, WebAPIOntologyException, LocalOntologyException
except ImportError:
    class Ontology():
        """
        Stub of abstract PyNERT ontology base class for implementing ontologies used if PyNERT is not installed.
        """
        def __init__(self, version: str, ontology_name: str, ontology_uri: str):
            self.version = version
            self.ontology_name = ontology_name
            self.ontology_uri = ontology_uri

        def get_ontology_entity(self, key: str):
            """
            Empty stub method.
            :raises KeyError: Since no keys are in the stup the function always raises KeyError
            """
            raise KeyError("Key not in ontology. PyNERD not installed.")


    class OntologyEntityException(Exception):
       pass

    class WebAPIOntologyException(OntologyEntityException):
        pass

    class LocalOntologyException(OntologyEntityException):
        pass
