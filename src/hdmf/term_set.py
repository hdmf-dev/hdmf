import glob
import os
from collections import namedtuple
from .utils import docval
import warnings


class TermSet():
    """
    Class for implementing term sets from ontologies and other resources used to define the
    meaning and/or identify of terms.

    :ivar term_schema_path: The LinkML YAML enumeration schema
    :ivar sources: The prefixes for the ontologies used in the TermSet
    :ivar view: SchemaView of the term set schema
    """
    def __init__(self,
                 term_schema_path: str=None,
                 schemasheets_folder: str=None,
                 dynamic: bool=False
                 ):
        """
        :param term_schema_path: The path to LinkML YAML enumeration schema
        """
        try:
            from linkml_runtime.utils.schemaview import SchemaView
        except ImportError:
            msg = "Install linkml_runtime"
            raise ValueError(msg)

        self.term_schema_path = term_schema_path
        self.schemasheets_folder = schemasheets_folder

        if self.schemasheets_folder is not None:
            if self.term_schema_path is not None:
                msg = "Cannot have both a path to a Schemasheets folder and a TermSet schema."
                raise ValueError(msg)
            else:
                self.term_schema_path = self.schemasheets_convert()
                self.view = SchemaView(self.term_schema_path)
        else:
            self.view = SchemaView(self.term_schema_path)
        self.expanded_term_set_path = None
        if dynamic:
            # reset view to now include the dynamically populated term_set
            self.expanded_term_set_path = self.enum_expander()
            self.view = SchemaView(self.expanded_term_set_path)

        self.sources = self.view.schema.prefixes

    def __repr__(self):
        re = "class: %s\n" % str(self.__class__)
        re += "term_schema_path: %s\n" % self.term_schema_path
        return re

    def __perm_value_key_info(self, perm_values_dict: dict, key: str):
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

    @docval({'name': 'term', 'type': str, 'doc': "term to be validated"})
    def validate(self, **kwargs):
        """
        Validate term in dataset towards a termset.
        """
        term = kwargs['term']
        try:
            self[term]
            return True
        except ValueError:
            return False

    @property
    def view_set(self):
        """
        Property method to return a view of all terms in the the LinkML YAML Schema.
        """
        enumeration = list(self.view.all_enums())[0]

        perm_values_dict = self.view.all_enums()[enumeration].permissible_values
        enum_dict = {}
        for perm_value_key in perm_values_dict.keys():
            enum_dict[perm_value_key] = self.__perm_value_key_info(perm_values_dict=perm_values_dict,
                                                                   key=perm_value_key)

        return enum_dict

    def __getitem__(self, term):
        """
        Method to retrieve a term and term information (LinkML description and LinkML meaning) from the set of terms.
        """
        enumeration = list(self.view.all_enums())[0]
        perm_values_dict = self.view.all_enums()[enumeration].permissible_values

        try:
            term_info = self.__perm_value_key_info(perm_values_dict=perm_values_dict, key=term)
            return term_info

        except KeyError:
            msg = 'Term not in schema'
            raise ValueError(msg)

    def schemasheets_convert(self):
        try:
            import yaml
            from linkml_runtime.utils.schema_as_dict import schema_as_dict
            from schemasheets.schemamaker import SchemaMaker
        except ImportError:   # pragma: no cover
            msg="Install schemasheets."   # pragma: no cover
            raise ValueError(msg)   # pragma: no cover
        schema_maker = SchemaMaker()
        tsv_file_paths = glob.glob(self.schemasheets_folder + "/*.tsv")
        schema = schema_maker.create_schema(tsv_file_paths)
        schema_dict = schema_as_dict(schema)
        schemasheet_schema_path = os.path.join(self.schemasheets_folder, f"{schema_dict['name']}.yaml")

        with open(schemasheet_schema_path, "w") as f:
            yaml.dump(schema_dict, f)

        return schemasheet_schema_path

    def enum_expander(self):
        try:
            warnings.filterwarnings("ignore", category=DeprecationWarning)
            from oaklib.utilities.subsets.value_set_expander import ValueSetExpander
        except ImportError:
            msg = 'Install oaklib.'
            raise ValueError(msg)
        expander = ValueSetExpander()
        # TODO: linkml should raise a warning if the schema does not have dynamic enums
        enum = list(self.view.all_enums())
        schema_dir = os.path.dirname(self.term_schema_path)
        file_name = os.path.basename(self.term_schema_path)
        output_path = os.path.join(schema_dir, f"expanded_{file_name}")
        expander.expand_in_place(self.term_schema_path, enum, output_path)

        return output_path
