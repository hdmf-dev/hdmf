import glob
import os
from collections import namedtuple
from .utils import docval
import warnings
import numpy as np
from .data_utils import append_data, extend_data


class TermSet:
    """
    Class for implementing term sets from ontologies and other resources used to define the
    meaning and/or identify of terms.

    :ivar term_schema_path: The path to the LinkML YAML enumeration schema
    :ivar sources: The prefixes for the ontologies used in the TermSet
    :ivar view: SchemaView of the term set schema
    :ivar schemasheets_folder: The path to the folder containing the LinkML TSV files
    :ivar expanded_termset_path: The path to the schema with the expanded enumerations
    """
    def __init__(self,
                 term_schema_path: str=None,
                 schemasheets_folder: str=None,
                 dynamic: bool=False
                 ):
        """
        :param term_schema_path: The path to the LinkML YAML enumeration schema
        :param schemasheets_folder: The path to the folder containing the LinkML TSV files
        :param dynamic: Boolean parameter denoting whether the schema uses Dynamic Enumerations

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
                self.term_schema_path = self.__schemasheets_convert()
                self.view = SchemaView(self.term_schema_path)
        else:
            self.view = SchemaView(self.term_schema_path)
        self.expanded_termset_path = None
        if dynamic:
            # reset view to now include the dynamically populated termset
            self.expanded_termset_path = self.__enum_expander()
            self.view = SchemaView(self.expanded_termset_path)

        self.name = self.view.schema.name
        self.sources = self.view.schema.prefixes

    def __repr__(self):
        terms = list(self.view_set.keys())

        re = "Schema Path: %s\n" % self.term_schema_path
        re += "Sources: " + ", ".join(list(self.sources.keys()))+"\n"
        re += "Terms: \n"
        if len(terms) > 4:
            re += "   - %s\n" % terms[0]
            re += "   - %s\n" % terms[1]
            re += "   - %s\n" % terms[2]
            re += "   ... ... \n"
            re += "   - %s\n" % terms[-1]
        else:
            for term in terms:
                re += "   - %s\n" % term
        re += "Number of terms: %s" % len(terms)
        return re

    def _repr_html_(self):
        terms = list(self.view_set.keys())

        re = "<b>" + "Schema Path: " + "</b>" + self.term_schema_path + "<br>"
        re += "<b>" + "Sources: " + "</b>" + ", ".join(list(self.sources.keys())) + "<br>"
        re += "<b> Terms: </b>"
        if len(terms) > 4:
            re += "<li> %s </li>" % terms[0]
            re += "<li> %s </li>" % terms[1]
            re += "<li> %s </li>" % terms[2]
            re += "... ..."
            re += "<li> %s </li>" % terms[-1]
        else:
            for term in terms:
                re += "<li> %s </li>" % term
        re += "<i> Number of terms:</i> %s" % len(terms)
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

    def __schemasheets_convert(self):
        """
        Method that will generate a schema from a directory of TSV files using SchemaMaker.

        This method returns a path to the new schema to be viewed via SchemaView.
        """
        try:
            import yaml
            from linkml_runtime.utils.schema_as_dict import schema_as_dict
            from schemasheets.schemamaker import SchemaMaker
        except ImportError:   # pragma: no cover
            msg = "Install schemasheets."
            raise ValueError(msg)
        schema_maker = SchemaMaker()
        tsv_file_paths = glob.glob(self.schemasheets_folder + "/*.tsv")
        schema = schema_maker.create_schema(tsv_file_paths)
        schema_dict = schema_as_dict(schema)
        schemasheet_schema_path = os.path.join(self.schemasheets_folder, f"{schema_dict['name']}.yaml")

        with open(schemasheet_schema_path, "w") as f:
            yaml.dump(schema_dict, f)

        return schemasheet_schema_path

    def __enum_expander(self):
        """
        Method that will generate a new schema with the enumerations from the LinkML source.
        This new schema will be stored in the same directory as the original schema with
        the Dynamic Enumerations.

        This method returns a path to the new schema to be viewed via SchemaView.
        """
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=DeprecationWarning)
                from oaklib.utilities.subsets.value_set_expander import ValueSetExpander
        except ImportError:   # pragma: no cover
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

class TermSetWrapper:
    """
    This class allows any HDF5 dataset or attribute to have a TermSet.
    """
    @docval({'name': 'termset',
             'type': TermSet,
             'doc': 'The TermSet to be used.'},
            {'name': 'value',
             'type': (list, np.ndarray, dict, str, tuple),
             'doc': 'The target item that is wrapped, either data or attribute.'},
            )
    def __init__(self, **kwargs):
        self.__value = kwargs['value']
        self.__termset = kwargs['termset']
        self.__validate()

    def __validate(self):
        # check if list, tuple, array
        if isinstance(self.__value, (list, np.ndarray, tuple)): # TODO: Future ticket on DataIO support
            values = self.__value
        # create list if none of those -> mostly for attributes
        else:
            values = [self.__value]
        # iteratively validate
        bad_values = []
        for term in values:
            validation = self.__termset.validate(term=term)
            if not validation:
                bad_values.append(term)
        if len(bad_values)!=0:
            msg = ('"%s" is not in the term set.' % ', '.join([str(value) for value in bad_values]))
            raise ValueError(msg)

    @property
    def value(self):
        return self.__value

    @property
    def termset(self):
        return self.__termset

    @property
    def dtype(self):
        return self.__getattr__('dtype')

    def __getattr__(self, val):
        """
        This method is to get attributes that are not defined in init.
        This is when dealing with data and numpy arrays.
        """
        return getattr(self.__value, val)

    def __getitem__(self, val):
        """
        This is used when we want to index items.
        """
        return self.__value[val]

    # uncomment when DataChunkIterator objects can be wrapped by TermSet
    # def __next__(self):
    #     """
    #     Return the next item of a wrapped iterator.
    #     """
    #     return self.__value.__next__()
    #
    def __len__(self):
        return len(self.__value)

    def __iter__(self):
        """
        We want to make sure our wrapped items are still iterable.
        """
        return self.__value.__iter__()

    def append(self, arg):
        """
        This append resolves the wrapper to use the append of the container using
        the wrapper.
        """
        if self.termset.validate(term=arg):
            self.__value = append_data(self.__value, arg)
        else:
            msg = ('"%s" is not in the term set.' % arg)
            raise ValueError(msg)

    def extend(self, arg):
        """
        This append resolves the wrapper to use the extend of the container using
        the wrapper.
        """
        bad_data = []
        for item in arg:
            if not self.termset.validate(term=item):
                bad_data.append(item)

        if len(bad_data)==0:
            self.__value = extend_data(self.__value, arg)
        else:
            msg = ('"%s" is not in the term set.' % ', '.join([str(item) for item in bad_data]))
            raise ValueError(msg)
