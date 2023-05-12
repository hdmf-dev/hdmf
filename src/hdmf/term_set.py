from linkml_runtime.utils.schemaview import SchemaView
from collections import namedtuple


class TermSet():
    """
    Class for implementing term sets from ontologies.

    :ivar name: The name of the TermSet
    :ivar term_schema_path: The LinkML YAML enumeration schema
    :ivar ontology_version: The version of the ontology
    :ivar ontology_source_name:  The name of the ontology
    :ivar ontology_source_uri: The uri of the ontology
    """
    def __init__(self,
                 name: str,
                 term_schema_path: str,
                 # ontology_version: str,
                 ):
        """
        :param name: The name of the TermSet
        :param term_schema_path: The path to LinkML YAML enumeration schema
        :param ontology_version: The version of the ontology
        :param ontology_source_name: The name of the ontology
        :param ontology_source_uri: The uri of the ontology
        """
        self.name = name
        self.term_schema_path = term_schema_path
        self.view = SchemaView(self.term_schema_path)
        self.sources = self.view.schema.prefixes

    def __repr__(self):
        re = "class: %s\n" % str(self.__class__)
        re += "name: %s\n" % self.name
        re += "term_schema_path: %s\n" % self.term_schema_path
        re += "ontology_source_name: %s\n" % self.sources
        return re

    def _perm_value_key_info(self, perm_values_dict: dict, key: str):
        """
        Private method to retrieve the id, description, and the meaning.
        """
        prefix_dict = self.view.schema.prefixes
        info_tuple = namedtuple("Term_Info", ["id", "description", "meaning"])
        description = perm_values_dict[key]['description']
        enum_meaning = perm_values_dict[key]['meaning']

        # filter for prefixes
        marker = ':'
        prefix = enum_meaning.split(marker, 1)[0]
        id = enum_meaning.split(marker, 1)[1]
        prefix_obj = prefix_dict[prefix]
        prefix_reference = prefix_obj['prefix_reference']

        # combine prefix and prefix_reference to make full term uri
        meaning = prefix_reference+id

        return info_tuple(enum_meaning, description, meaning)

    @property
    def view_set(self):
        """
        Property method to return a view of all terms in the the LinkML YAML Schema.
        """
        enumeration = list(self.view.all_enums())[0]

        perm_values_dict = self.view.all_enums()[enumeration].permissible_values
        enum_dict = {}
        for perm_value_key in perm_values_dict.keys():
            enum_dict[perm_value_key] = self._perm_value_key_info(perm_values_dict=perm_values_dict, key=perm_value_key)

        return enum_dict

    def __getitem__(self, term):
        """
        Method to retrieve a term and term information (LinkML description and LinkML meaning) from the set of terms.
        """
        enumeration = list(self.view.all_enums())[0]
        perm_values_dict = self.view.all_enums()[enumeration].permissible_values

        try:
            term_info = self._perm_value_key_info(perm_values_dict=perm_values_dict, key=term)
            return term_info

        except KeyError:
            msg = 'Term not in schema'
            raise ValueError(msg)
