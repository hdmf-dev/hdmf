"""
ExternalResources
=================

This is a user guide to interacting with the
:py:class:`~hdmf.common.resources.ExternalResources` class. The ExternalResources type
is experimental and is subject to change in future releases. If you use this type,
please provide feedback to the HDMF team so that we can improve the structure and
access of data stored with this type for your use cases.

Introduction
-------------
The :py:class:`~hdmf.common.resources.ExternalResources` class provides a way
to organize and map user terms from their data (keys) to multiple entities
from the external resources. A typical use case for external resources is to link data
stored in datasets or attributes to ontologies. For example, you may have a
dataset ``country`` storing locations. Using
:py:class:`~hdmf.common.resources.ExternalResources` allows us to link the
country names stored in the dataset to an ontology of all countries, enabling
more rigid standardization of the data and facilitating data query and
introspection.

From a user's perspective, one can think of the
:py:class:`~hdmf.common.resources.ExternalResources` as a simple table, in which each
row associates a particular ``key`` stored in a particular ``object`` (i.e., Attribute
or Dataset in a file) with a particular ``entity`` (i.e, a term of an online
resource). That is, ``(object, key)`` refer to parts inside a
file and ``entity`` refers to an external resource outside the file, and
:py:class:`~hdmf.common.resources.ExternalResources` allows us to link the two. To
reduce data redundancy and improve data integrity,
:py:class:`~hdmf.common.resources.ExternalResources` stores this data internally in a
collection of interlinked tables.

* :py:class:`~hdmf.common.resources.KeyTable` where each row describes a
  :py:class:`~hdmf.common.resources.Key`
* :py:class:`~hdmf.common.resources.FileTable` where each row describes a
  :py:class:`~hdmf.common.resources.File`
* :py:class:`~hdmf.common.resources.EntityTable` where each row describes an
  :py:class:`~hdmf.common.resources.Entity`
* :py:class:`~hdmf.common.resources.EntityKeyTable` where each row describes an
  :py:class:`~hdmf.common.resources.EntityKey`
* :py:class:`~hdmf.common.resources.ObjectTable` where each row describes an
  :py:class:`~hdmf.common.resources.Object`
* :py:class:`~hdmf.common.resources.ObjectKeyTable` where each row describes an
  :py:class:`~hdmf.common.resources.ObjectKey` pair identifying which keys
  are used by which objects.

The :py:class:`~hdmf.common.resources.ExternalResources` class then provides
convenience functions to simplify interaction with these tables, allowing users
to treat :py:class:`~hdmf.common.resources.ExternalResources` as a single large table as
much as possible.

Rules to ExternalResources
---------------------------
When using the :py:class:`~hdmf.common.resources.ExternalResources` class, there
are rules to how users store information in the interlinked tables.

1. Multiple :py:class:`~hdmf.common.resources.Key` objects can have the same name.
   They are disambiguated by the :py:class:`~hdmf.common.resources.Object` associated
   with each, meaning we may have keys with the same name in different objects, but for a particular object
   all keys must be unique.
2. In order to query specific records, the :py:class:`~hdmf.common.resources.ExternalResources` class
   uses '(file, object_id, relative_path, field, key)' as the unique identifier.
3. :py:class:`~hdmf.common.resources.Object` can have multiple :py:class:`~hdmf.common.resources.Key`
   objects.
4. Multiple :py:class:`~hdmf.common.resources.Object` objects can use the same :py:class:`~hdmf.common.resources.Key`.
5. Do not use the private methods to add into the :py:class:`~hdmf.common.resources.KeyTable`,
   :py:class:`~hdmf.common.resources.ResourceTable`, :py:class:`~hdmf.common.resources.EntityTable`,
   :py:class:`~hdmf.common.resources.ObjectTable`, :py:class:`~hdmf.common.resources.ObjectKeyTable`
   individually.
6. URIs are optional, but highly recommended. If not known, an empty string may be used.
7. An entity ID should be the unique string identifying the entity in the given resource.
   This may or may not include a string representing the resource and a colon.
   Use the format provided by the resource. For example, Identifiers.org uses the ID ``ncbigene:22353``
   but the NCBI Gene uses the ID ``22353`` for the same term.
8. In a majority of cases, :py:class:`~hdmf.common.resources.Object` objects will have an empty string
   for 'field'. The :py:class:`~hdmf.common.resources.ExternalResources` class supports compound data_types.
   In that case, 'field' would be the field of the compound data_type that has an external reference.
9. In some cases, the attribute that needs an external reference is not a object with a 'data_type'.
   The user must then use the nearest object that has a data type to be used as the parent object. When
   adding an external resource for an object with a data type, users should not provide an attribute.
   When adding an external resource for an attribute of an object, users need to provide
   the name of the attribute.
10. The user must provide a :py:class:`~hdmf.common.resources.File` or an :py:class:`~hdmf.common.resources.Object` that
    has :py:class:`~hdmf.common.resources.File` along the parent hierarchy.
"""
######################################################
# Creating an instance of the ExternalResources class
# ----------------------------------------------------

# sphinx_gallery_thumbnail_path = 'figures/gallery_thumbnail_externalresources.png'
from hdmf.common import ExternalResources
from hdmf.common import DynamicTable, VectorData
from hdmf import Container, ExternalResourcesManager
from hdmf import Data
import numpy as np
import os
# Ignore experimental feature warnings in the tutorial to improve rendering
import warnings
warnings.filterwarnings("ignore", category=UserWarning, message="ExternalResources is experimental*")


# Class to represent a file
class ExternalResourcesManagerContainer(Container, ExternalResourcesManager):
    def __init__(self, **kwargs):
        kwargs['name'] = 'ExternalResourcesManagerContainer'
        super().__init__(**kwargs)


er = ExternalResources()
file = ExternalResourcesManagerContainer(name='file')


###############################################################################
# Using the add_ref method
# ------------------------------------------------------
# :py:func:`~hdmf.common.resources.ExternalResources.add_ref`
# is a wrapper function provided by the
# :py:class:`~hdmf.common.resources.ExternalResources` class that simplifies adding
# data. Using :py:func:`~hdmf.common.resources.ExternalResources.add_ref` allows us to
# treat new entries similar to adding a new row to a flat table, with
# :py:func:`~hdmf.common.resources.ExternalResources.add_ref` taking care of populating
# the underlying data structures accordingly.

data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
er.add_ref(
    file=file,
    container=data,
    key='Homo sapiens',
    entity_id='NCBI_TAXON:9606',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606'
)

er.add_ref(
    file=file,
    container=data,
    key='Mus musculus',
    entity_id='NCBI_TAXON:10090',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090'
)

###############################################################################
# Using the add_ref method with an attribute
# ------------------------------------------------------
# It is important to keep in mind that when adding and :py:class:`~hdmf.common.resources.Object` to
# the :py:class:~hdmf.common.resources.ObjectTable, the parent object identified by
# :py:class:`~hdmf.common.resources.Object.object_id` must be the closest parent to the target object
# (i.e., :py:class:`~hdmf.common.resources.Object.relative_path` must be the shortest possible path and
# as such cannot contain any objects with a ``data_type`` and associated ``object_id``).
#
# A common example would be with the :py:class:`~hdmf.common.table.DynamicTable` class, which holds
# :py:class:`~hdmf.common.table.VectorData` objects as columns. If we wanted to add an external
# reference on a column from a :py:class:`~hdmf.common.table.DynamicTable`, then we would use the
# column as the object and not the :py:class:`~hdmf.common.table.DynamicTable` (Refer to rule 9).

genotypes = DynamicTable(name='genotypes', description='My genotypes')
genotypes.add_column(name='genotype_name', description="Name of genotypes")
genotypes.add_row(id=0, genotype_name='Rorb')
er.add_ref(
    file=file,
    container=genotypes,
    attribute='genotype_name',
    key='Rorb',
    entity_id='MGI:1346434',
    entity_uri='http://www.informatics.jax.org/marker/MGI:1343464'
)

# Note: :py:func:`~hdmf.common.resources.ExternalResources.add_ref` internally resolves the object
# to the closest parent, so that ``er.add_ref(container=genotypes, attribute='genotype_name')`` and
# ``er.add_ref(container=genotypes.genotype_name, attribute=None)`` will ultimately both use the ``object_id``
# of the ``genotypes.genotype_name`` :py:class:`~hdmf.common.table.VectorData` column and
# not the object_id of the genotypes table.

###############################################################################
# Using the add_ref method without the file parameter.
# ------------------------------------------------------
# Even though :py:class:`~hdmf.common.resources.File` is required to create/add a new reference,
# the user can omit the file parameter if the :py:class:`~hdmf.common.resources.Object` has a file
# in its parent hierarchy.

col1 = VectorData(
    name='Species_Data',
    description='species from NCBI and Ensemble',
    data=['Homo sapiens', 'Ursus arctos horribilis'],
)

# Create a DynamicTable with this column and set the table parent to the file object created earlier
species = DynamicTable(name='species', description='My species', columns=[col1])
species.parent = file

er.add_ref(
    container=species,
    attribute='Species_Data',
    key='Ursus arctos horribilis',
    entity_id='NCBI_TAXON:116960',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id'
)

###############################################################################
# Visualize ExternalResources
# ------------------------------------------------------
# Users can visualize `~hdmf.common.resources.ExternalResources` as a flattened table or
# as separate tables.

# `~hdmf.common.resources.ExternalResources` as a flattened table
er.to_dataframe()

# The individual interlinked tables:
er.files.to_dataframe()
er.objects.to_dataframe()
er.entities.to_dataframe()
er.keys.to_dataframe()
er.object_keys.to_dataframe()
er.entity_keys.to_dataframe()

###############################################################################
# Using the get_key method
# ------------------------------------------------------
# The :py:func:`~hdmf.common.resources.ExternalResources.get_key`
# method will return a :py:class:`~hdmf.common.resources.Key` object. In the current version of
# :py:class:`~hdmf.common.resources.ExternalResources`, duplicate keys are allowed; however, each key needs a unique
# linking Object. In other words, each combination of (file, container, relative_path, field, key)
# can exist only once in :py:class:`~hdmf.common.resources.ExternalResources`.

# The :py:func:`~hdmf.common.resources.ExternalResources.get_key` method will be able to return the
# :py:class:`~hdmf.common.resources.Key` object if the :py:class:`~hdmf.common.resources.Key` object is unique.
genotype_key_object = er.get_key(key_name='Rorb')

# If the :py:class:`~hdmf.common.resources.Key` object has a duplicate name, then the user will need
# to provide the unique (file, container, relative_path, field, key) combination.
species_key_object = er.get_key(file=file,
                                container=species['Species_Data'],
                                key_name='Ursus arctos horribilis')

# The :py:func:`~hdmf.common.resources.ExternalResources.get_key` also will check the
# :py:class:`~hdmf.common.resources.Object` for a :py:class:`~hdmf.common.resources.File` along the parent hierarchy
# if the file is not provided as in :py:func:`~hdmf.common.resources.ExternalResources.add_ref`

###############################################################################
# Using the add_ref method with a key_object
# ------------------------------------------------------
# Multiple :py:class:`~hdmf.common.resources.Object` objects can use the same
# :py:class:`~hdmf.common.resources.Key`. To use an existing key when adding
# new entries into :py:class:`~hdmf.common.resources.ExternalResources`, pass the
# :py:class:`~hdmf.common.resources.Key` object instead of the 'key_name' to the
# :py:func:`~hdmf.common.resources.ExternalResources.add_ref` method. If a 'key_name'
# is used, a new :py:class:`~hdmf.common.resources.Key` will be created.

er.add_ref(
    file=file,
    container=genotypes,
    attribute='genotype_name',
    key=genotype_key_object,
    entity_id='ENSEMBL:ENSG00000198963',
    entity_uri='https://uswest.ensembl.org/Homo_sapiens/Gene/Summary?db=core;g=ENSG00000198963'
)

###############################################################################
# Using the get_object_entities
# ------------------------------------------------------
# The :py:class:`~hdmf.common.resources.ExternalResources.get_object_entities` method
# allows the user to retrieve all entities and key information associated with an `Object` in
# the form of a pandas DataFrame.

er.get_object_entities(file=file,
                       container=genotypes['genotype_name'],
                       relative_path='')

###############################################################################
# Using the get_object_type
# ------------------------------------------------------
# The :py:class:`~hdmf.common.resources.ExternalResources.get_object_entities` method
# allows the user to retrieve all entities and key information associated with an `Object` in
# the form of a pandas DataFrame.

er.get_object_type(object_type='Data')

###############################################################################
# Special Case: Using add_ref with compound data
# ------------------------------------------------
# In most cases, the field is left as an empty string, but if the dataset or attribute
# is a compound data_type, then we can use the 'field' value to differentiate the
# different columns of the dataset. For example, if a dataset has a compound data_type with
# columns/fields 'x', 'y', and 'z', and each
# column/field is associated with different ontologies, then use field='x' to denote that
# 'x' is using the external reference.

# Let's create a new instance of :py:class:`~hdmf.common.resources.ExternalResources`.
er = ExternalResources()
file = ExternalResourcesManagerContainer(name='file')

data = Data(
    name='data_name',
    data=np.array(
        [('Mus musculus', 9, 81.0), ('Homo sapiens', 3, 27.0)],
        dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]
    )
)

er.add_ref(
    file=file,
    container=data,
    field='species',
    key='Mus musculus',
    entity_id='NCBI_TAXON:txid10090',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090'
)

###############################################################################
# Write ExternalResources
# ------------------------------------------------------
# :py:class:`~hdmf.common.resources.ExternalResources` is written as a zip file of
# the individual tables written to tsv.
# The user provides the path, which contains the name of the directory.

er.to_norm_tsv(path='./')

###############################################################################
# Read ExternalResources
# ------------------------------------------------------
# Users can read :py:class:`~hdmf.common.resources.ExternalResources` from the tsv format
# by providing the path to the directory.

er_read = ExternalResources.from_norm_tsv(path='./')
os.remove('./er.zip')

###############################################################################
# Using TermSet with ExternalResources
# ------------------------------------------------
# :py:class:`~hdmf.term_set.TermSet` allows for an easier way to add references to
# :py:class:`~hdmf.common.resources.ExternalResources`. These enumerations take place of the
# entity_id and entity_uri parameters. :py:class:`~hdmf.common.resources.Key` values will have
# to match the name of the term in the :py:class:`~hdmf.term_set.TermSet`.
from hdmf.term_set import TermSet

try:
    dir_path = os.path.dirname(os.path.abspath(__file__))
    yaml_file = os.path.join(dir_path, 'example_term_set.yaml')
except NameError:
    dir_path = os.path.dirname(os.path.abspath('.'))
    yaml_file = os.path.join(dir_path, 'gallery/example_term_set.yaml')

terms = TermSet(term_schema_path=yaml_file)
col1 = VectorData(
    name='Species_Data',
    description='...',
    data=['Homo sapiens', 'Ursus arctos horribilis'],
    term_set=terms,
)

species = DynamicTable(name='species', description='My species', columns=[col1],)
er.add_ref_term_set(file=file,
                    container=species,
                    attribute='Species_Data',
                   )
