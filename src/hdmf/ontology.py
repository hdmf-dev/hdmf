import requests
from .utils import docval, popargs, call_docval_func, get_docval

class Ontology():
    """

    """

    @docval({'name': 'version', 'type': str, 'doc': 'The version of the ontology.'},
            {'name': 'ontology_name', 'type': str, 'doc': 'The name of the ontology/the resource from ExternalResources.'},
            {'name': 'ontology_uri', 'type': str, 'doc': 'The uri of the ontology/the resource from ExternalResources.'})
    def __init__(self, **kwargs):
        self.version, self.ontology_name, self.ontology_uri = popargs('version', 'ontology_name', 'ontology_uri', kwargs)


class WebAPIOntology(Ontology): #webapi
    """

    """
    @docval(*get_docval(Ontology.__init__, 'version', 'ontology_name', 'ontology_uri'),
            {'name': 'extension', 'type': str, 'doc': 'URI extension to the ontology URI'})
    def __init__(self, **kwargs):
        call_docval_func(super().__init__, kwargs)
        self.extension = kwargs['extension']

    @docval({'name': 'key', 'type': str, 'doc': 'The key name from the object to return the ontology entity.'})
    def get_ontology_entity(self, **kwargs):
        key = kwargs['key']
        entity_uri = self.ontology_uri+self.extension+key

        request = requests.get(entity_uri, headers={ "Content-Type" : "application/json"})
        if not request.ok:
            msg = ("Invalid Request")
            raise ValueError(msg)
        else:
            request_json = request.json()
            entity_id = request_json['id']
            return entity_id, entity_uri

class LocalOntology(Ontology):
    """

    """
    @docval(*get_docval(Ontology.__init__, 'version', 'ontology_name', 'ontology_uri'),
            {'name': 'ontology_entities', 'type': dict, 'doc': 'Dictionary of ontology terms with corresponding ID and uri', 'default': {}})
    def __init__(self, **kwargs):
        call_docval_func(super().__init__, kwargs)
        self.ontology_entities = kwargs['ontology_entities']

    @docval({'name': 'key', 'type': str, 'doc': 'The new ontology term to be added'},
            {'name': 'entity_value', 'type': (list, tuple), 'doc': 'A list or tuple of the new entity ID and URO'})
    def add_ontology_entity(self, **kwargs):
        key = kwargs['key']
        entity_value = kwargs['entity_value']

        self.ontology_entities[key] = entity_value
        return self.ontology_entities

    @docval({'name': 'key', 'type': str, 'doc': 'The ontology term to be removed'})
    def remove_ontology_entity(self, **kwargs):
        key = kwargs['key']

        self.ontology_entities.pop(key)
        return self.ontology_entities

    @docval({'name': 'key', 'type': str, 'doc': 'The ontology term to be retrieved'})
    def get_ontology_entity(self, **kwargs):
        key = kwargs['key']
        entity_id, entity_uri = self.ontology_entities[key]
        return entity_id, entity_uri

class EnsemblOntology(WebAPIOntology):
    """

    """

    ontology_name = 'Ensembl'

    @docval(*get_docval(WebAPIOntology.__init__, 'version'),
            {'name': 'ontology_uri', 'type': str, 'doc': 'The uri of the ontology/the resource from ExternalResources.', 'default': 'https://rest.ensembl.org'},
            {'name': 'extension', 'type': str, 'doc': 'URI extension to the ontology URI', 'default': '/taxonomy/id/'})
    def __init__(self, **kwargs):
        self.version, self.ontology_uri, self.extension = popargs('version', 'ontology_uri', 'extension', kwargs)

class NCBI_Taxonomy(LocalOntology):
    """

    """

    ontology_name = 'NCBI_Taxonomy'
    ontology_entities = {"Homo sapiens": ['9606', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606']}

    @docval(*get_docval(LocalOntology.__init__, 'version'),
            {'name': 'ontology_uri', 'type': str, 'doc': 'The NCBI Taxonomy uri', 'default': 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi'})
    def __init__(self, **kwargs):
        self.version, self.ontology_uri = popargs('version', 'ontology_uri', kwargs)
