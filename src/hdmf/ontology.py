import requests
from .utils import docval, popargs, call_docval_func, get_docval

class Ontology():
    """

    """

    __fields__ = (
        'name',
        'ontology_name',
        'ontology_uri',
        'extension'
    )

    @docval({'name': 'name', 'type': str, 'doc': 'The name of this ontology Container.'},
            {'name': 'ontology_name', 'type': str, 'doc': 'The name of the ontology/the resource from ExternalResources.'},
            {'name': 'ontology_uri', 'type': str, 'doc': 'The uri of the ontology/the resource from ExternalResources.'},
            {'name': 'extension', 'type': str, 'doc': 'URI extension to the ontology URI', 'default': None})
    def __init__(self, **kwargs):
        self.name, self.ontology_name, self.ontology_uri, self.extension = popargs('name', 'ontology_name', 'ontology_uri', 'extension', kwargs)


class BrowserOntology(Ontology):
    """

    """

    @docval({'name': 'name', 'type': str, 'doc': 'The name of this ontology Container.'},
            {'name': 'ontology_name', 'type': str, 'doc': 'The name of the ontology/the resource from ExternalResources.'},
            {'name': 'ontology_uri', 'type': str, 'doc': 'The uri of the ontology/the resource from ExternalResources.'},
            {'name': 'extension', 'type': str, 'doc': 'URI extension to the ontology URI', 'default': None})
    def __init__(self, **kwargs):
        call_docval_func(super().__init__, kwargs)

    @docval({'name': 'key', 'type': str, 'doc': 'The key name from the object to return the ontology entity.'})
    def get_entity_browser(self, **kwargs):
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


class EnsemblOntology(BrowserOntology):
    """

    """

    ontology_name = 'Ensembl'

    @docval(*get_docval(Ontology.__init__, 'name'),
            {'name': 'ontology_uri', 'type': str, 'doc': 'The uri of the ontology/the resource from ExternalResources.', 'default': 'https://rest.ensembl.org'},
            {'name': 'extension', 'type': str, 'doc': 'URI extension to the ontology URI', 'default': '/taxonomy/id/'})
    def __init__(self, **kwargs):
        self.name = kwargs['name']
        self.ontology_uri = kwargs['ontology_uri']
        self.extension = kwargs['extension']


    @docval({'name': 'key', 'type': str, 'doc': 'The key name from the object to return the ontology entity.'})
    def get_entity_browser(self, **kwargs):
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
