import requests
from .utils import docval, popargs, call_docval_func, get_docval
from abc import abstractmethod
from .errors import WebAPIOntologyException, LocalOntologyException
import ruamel.yaml as yaml
import os


class Ontology():
    """

    """
    #do we need to record the version? ask pam and co.
    @docval({'name': 'version', 'type': str, 'doc': 'The version of the ontology.'},
            {'name': 'ontology_name', 'type': str, 'doc': 'The name of the ontology/the resource from ExternalResources.'},
            {'name': 'ontology_uri', 'type': str, 'doc': 'The uri of the ontology/the resource from ExternalResources.'})
    def __init__(self, **kwargs):
        self.version, self.ontology_name, self.ontology_uri = popargs('version', 'ontology_name', 'ontology_uri', kwargs)

    @abstractmethod
    @docval({'name': 'key', 'type': str, 'doc': 'The key name from the object to return the ontology entity.'})
    def get_ontology_entity(self, **kwargs):
        pass


class LocalOntology(Ontology):
    """

    """
    @docval(*get_docval(Ontology.__init__, 'version', 'ontology_name', 'ontology_uri'),
            {'name': '_ontology_entities', 'type': dict, 'doc': 'Dictionary of ontology terms with corresponding ID and uri as a tuple/list', 'default': {}})
    def __init__(self, **kwargs):
        call_docval_func(super().__init__, kwargs)
        self._ontology_entities = kwargs['_ontology_entities']
        # self.write_ontology_yaml()

    # have a function to save and pull up dict as yaml
    @docval({'name': 'path', 'type': str, 'doc': 'A path of the local ontology', 'default': None},
            {'name': 'ontology_dict', 'type': dict, 'doc': 'A dict of the local ontology', 'default': None})
    def write_ontology_yaml(self, **kwargs):
        path = kwargs['path']
        ontology_dict = kwargs['ontology_dict']

        if path is None:
            path = os.path.dirname(os.path.abspath(__file__)) + "/local_" + self.ontology_name + ".yaml"
        if ontology_dict is None:
            ontology_dict=self._ontology_entities

        with open(path, 'w+') as file:
            yaml_obj=yaml.YAML(typ='safe', pure=True)
            documents = yaml_obj.dump(ontology_dict, file)
            return documents

    @docval({'name': 'path', 'type': str, 'doc': 'A path of the local ontology'})
    def read_ontology_yaml(self, **kwargs):
        path = kwargs['path']

        with open(path) as file:
            yaml_obj=yaml.YAML(typ='safe', pure=True)
            documents = yaml_obj.load(file)
            return documents

    @docval({'name': 'key', 'type': str, 'doc': 'The new ontology term to be added'},
            {'name': 'entity_value', 'type': (list, tuple), 'doc': 'A list or tuple of the new entity ID and URO'})
    def add_ontology_entity(self, **kwargs):
        key = kwargs['key']
        entity_value = kwargs['entity_value']

        if len(entity_value)==2 and entity_value[1][:4]=='http':
            self._ontology_entities[key] = entity_value
            return self._ontology_entities
        else:
            msg = 'New entity does not match format requirements'
            raise ValueError(msg)

    @docval({'name': 'key', 'type': str, 'doc': 'The ontology term to be removed'})
    def remove_ontology_entity(self, **kwargs):
        key = kwargs['key']

        self._ontology_entities.pop(key)
        return self._ontology_entities

    @docval(*get_docval(Ontology.get_ontology_entity, 'key'))
    def get_ontology_entity(self, **kwargs):
        key = kwargs['key']
        try:
            entity_id, entity_uri = self._ontology_entities[key]
        except KeyError:
            raise LocalOntologyException
        else:
            return entity_id, entity_uri


class WebAPIOntology(LocalOntology):
    """

    """
    @docval(*get_docval(LocalOntology.__init__, 'version', 'ontology_name', 'ontology_uri', '_ontology_entities'),
            {'name': 'extension', 'type': str, 'doc': 'URI extension to the ontology URI'})
    def __init__(self, **kwargs):
        call_docval_func(super().__init__, kwargs)
        self.extension = kwargs['extension']

    @docval(*get_docval(Ontology.get_ontology_entity, 'key'))
    def get_ontology_entity(self, **kwargs):
        key = kwargs['key']
        entity_uri = self.ontology_uri+self.extension+key

        try:
            entity_id, entity_uri = self._ontology_entities[key]
        except KeyError:
            request = requests.get(entity_uri, headers={ "Content-Type" : "application/json"})
            if not request.ok:
                raise WebAPIOntologyException()
            else:
                request_json = request.json()
                entity_id = request_json['id']
                return entity_id, entity_uri
        else:
            return entity_id, entity_uri


class EnsemblOntology(WebAPIOntology):
    """

    """

    ontology_name = 'Ensembl'

    @docval(*get_docval(WebAPIOntology.__init__, 'version', '_ontology_entities'),
            {'name': 'ontology_uri', 'type': str, 'doc': 'The uri of the ontology/the resource from ExternalResources.', 'default': 'https://rest.ensembl.org'},
            {'name': 'extension', 'type': str, 'doc': 'URI extension to the ontology URI', 'default': '/taxonomy/id/'})
    def __init__(self, **kwargs):
        self.version, self.ontology_uri, self.extension, self._ontology_entities = popargs('version', 'ontology_uri', 'extension', '_ontology_entities', kwargs)

class NCBI_Taxonomy(LocalOntology):
    """

    """

    ontology_name = 'NCBI_Taxonomy'
    _ontology_entities = {"Homo sapiens": ['9606', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606']}

    @docval(*get_docval(LocalOntology.__init__, 'version'),
            {'name': 'ontology_uri', 'type': str, 'doc': 'The NCBI Taxonomy uri', 'default': 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi'})
    def __init__(self, **kwargs):
        self.version, self.ontology_uri = popargs('version', 'ontology_uri', kwargs)
