from .utils import docval

class OntologyEntityException(Exception):
    @docval({'name': 'message', 'type': str, 'doc': 'the error message'})
    def __init__(self, **kwargs):
        self.__message = kwargs['message']
        super().__init__(self.__message)

class WebAPIOntologyException(OntologyEntityException):
    def __init__(self, **kwargs):
        self.__message = "Invalid API key"
        super().__init__(self.__message)

class LocalOntologyException(OntologyEntityException):
    def __init__(self, **kwargs):
        self.__message = "Invalid local ontology key"
        super().__init__(self.__message)
