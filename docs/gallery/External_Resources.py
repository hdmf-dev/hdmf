"""
ExternalResources
=============================

This is a user guide to interacting with the ``ExternalResources`` class.
The ExternalResources type is experimental and is subject to change in future releases.
If you use this type, please provide feedback to the HDMF team so that we can
improve the structure and access of data stored with this type for your use cases.

"""

###############################################################################
# Introduction
# ------------------------------------------------------
# The :py:class:`~hdmf.common.resources.ExternalResources` class provides a way
# to organize and map user terms (keys) to multiple resources and entities
# from the resources. A typical use case for external resources is to link data
# stored in datasets or attributes to ontologies. For example, you may have a
# dataset ``country`` storing locations. Using
# :py:class:`~hdmf.common.resources.ExternalResources` allows us to link the
# country names stored in the dataset to an ontology of all countries, enabling
# more rigid standardization of the data and facilitating data query and
# introspection.
#
# From a user's perspective, one can think of the ``ExternalResources`` as a
# simple table, in which each row associates a particular ``key`` stored in a
# particular ``object`` (i.e., Attribute or Dataset in a file) with a particular
# ``entity`` (e.g., a term)  of an online ``resource`` (e.g., an ontology).
# That is, ``(object, key)`` refer to parts inside a file and ``(resource, entity)``
# refer to an external resource outside of the file, and ``ExternalResources``
# allows us to link the two. To reduce data redundancy and improve data integrity,
# ``ExternalResources`` stores this data internally in a collection of
# interlinked tables.
# * :py:class:`~hdmf.common.resources.KeyTable` where each row describes a
#   :py:class:`~hdmf.common.resources.Key`
# * :py:class:`~hdmf.common.resources.ResourceTable` where each row describes a
#   :py:class:`~hdmf.common.resources.Resource`
# * :py:class:`~hdmf.common.resources.EntityTable`  where each row describes an
#   :py:class:`~hdmf.common.resources.Entity`
# * :py:class:`~hdmf.common.resources.ObjectTable` where each row descibes an
#   :py:class:`~hdmf.common.resources.Object`
# * :py:class:`~hdmf.common.resources.ObjectKeyTable` where each row describes an
#   :py:class:`~hdmf.common.resources.ObjectKey` pair identifying which keys
#   are used by which objects.
#
# The :py:class:`~hdmf.common.resources.ExternalResources` class then provides
# convenience functions to simplify interaction with these tables, allowing users
# to treat ``ExternalResources`` as a single large table as much as possible.

###############################################################################
# Rules to ExternalResources
# ------------------------------------------------------
# When using the :py:class:`~hdmf.common.resources.ExternalResources` class, there
# are rules to how users store information in the interlinked tables.

# 1. Multiple :py:class:`~hdmf.common.resources.Key` objects can have the same name.
#    They are disambiguated by the :py:class:`~hdmf.common.resources.Object` associated
#    with each.
# 2. In order to query specific records, the :py:class:`~hdmf.common.resources.ExternalResources` class
#    uses '(object_id, relative_path, field, Key)' as the unique identifier.
# 3. :py:class:`~hdmf.common.resources.Object` can have multiple :py:class:`~hdmf.common.resources.Key`
#    objects.
# 4. Multiple :py:class:`~hdmf.common.resources.Object` objects can use the same :py:class:`~hdmf.common.resources.Key`.
#    Note that the :py:class:`~hdmf.common.resources.Key` may already be associated with resources
#    and entities.
# 5. Do not use the private methods to add into the :py:class:`~hdmf.common.resources.KeyTable`,
#    :py:class:`~hdmf.common.resources.ResourceTable`, :py:class:`~hdmf.common.resources.EntityTable`,
#    :py:class:`~hdmf.common.resources.ObjectTable`, :py:class:`~hdmf.common.resources.ObjectKeyTable`
#    individually.
# 6. URIs are optional, but highly recommended. If not known, an empty string may be used.
# 7. An entity ID should be the unique string identifying the entity in the given resource.
#    This may or may not include a string representing the resource and a colon.
#    Use the format provided by the resource. For example, Identifiers.org uses the ID ``ncbigene:22353``
#    but the NCBI Gene uses the ID ``22353`` for the same term.
# 8. In a majority of cases, :py:class:`~hdmf.common.resources.Object` objects will have an empty string
#    for 'field'. The :py:class:`~hdmf.common.resources.ExternalResources` class supports compound data_types.
#    In that case, 'field' would be the field of the compound data_type that has an external reference.
# 9. The :py:class:`~hdmf.common.resources.Object` object we add to the
#    :py:class:`~hdmf.common.resources.ObjectTable` has a data_type.
# 10. In some cases, the attribute that needs an external reference is not a object with a 'data_type'.
#     The user must then use the nearest object that has a data type to be used as the container.

###############################################################################
# Creating an instance of the ExternalResources class
# ------------------------------------------------------

# sphinx_gallery_thumbnail_path = 'figures/gallery_thumbnail_externalresources.png'
from hdmf.common import ExternalResources
from hdmf.common import DynamicTable
from hdmf import Data
import numpy as np

er = ExternalResources(name='example')

###############################################################################
# Using the add_ref method
# ------------------------------------------------------
# :py:func:`~hdmf.common.resources.ExternalResources.add_ref`
# is a wrapper function provided by the ``ExternalResources`` class that
# simplifies adding data. Using ``add_ref`` allows us to treat new entries similar
# to adding a new row to a flat table, with ``add_ref`` taking care of populating
# the underlying data structures accordingly.

data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
er.add_ref(
    container=data,
    key='Homo sapiens',
    resource_name='NCBI_Taxonomy',
    resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
    entity_id='NCBI:txid9606',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606'
)

er.add_ref(
    container=data,
    key='Mus musculus',
    resource_name='NCBI_Taxonomy',
    resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
    entity_id='NCBI:txid10090',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090'
)

###############################################################################
# Using the add_ref method with get_resource
# ------------------------------------------------------
# When adding references to resources, you may want to refer to multiple entities
# within the same resource. Resource names are unique, so if you call ``add_ref``
# with the name of an existing resource, then that resource will be reused. You
# can also use the :py:func:`~hdmf.common.resources.ExternalResources.get_resource`
# method to get the ``Resource`` object and pass that in to ``add_ref`` to
# reuse an existing resource.

# Let's create a new instance of ExternalResources.
er = ExternalResources(name='example')

data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
er.add_ref(
    container=data,
    key='Homo sapiens',
    resource_name='NCBI_Taxonomy',
    resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
    entity_id='NCBI:txid9606',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606'
)

# Using get_resource
existing_resource = er.get_resource('NCBI_Taxonomy')
er.add_ref(
    container=data,
    key='Mus musculus',
    resources_idx=existing_resource,
    entity_id='NCBI:txid10090',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090'
)

###############################################################################
# Using the add_ref method with Nested Objects
# ------------------------------------------------------
# It is important to keep in mind that the :py:class:`~hdmf.common.resources.Object` object
# we add to the :py:class:`~hdmf.common.resources.ObjectTable` is the object
# holding the attribute or the closest parent object. Recall that the objects in the
# :py:class:`~hdmf.common.resources.ObjectTable` must have a 'data_type'.
# A common example would be with the :py:class:`~hdmf.common.table.DynamicTable` class, which holds
# :py:class:`~hdmf.common.table.VectorData` objects as columns. If we wanted to add an external
# reference on a column from a :py:class:`~hdmf.common.table.DynamicTable`, then we would use the
# column as the object and not the :py:class:`~hdmf.common.table.DynamicTable`.

genotypes = DynamicTable(name='genotypes', description='My genotypes')
genotypes.add_column(name='genotype_name', description="Name of genotypes")
genotypes.add_row(id=0, genotype_name='Rorb')
er.add_ref(
    container=genotypes.columns[0],
    key='Rorb',
    resource_name='MGI Database',
    resource_uri='http://www.informatics.jax.org/',
    entity_id='MGI:1346434',
    entity_uri='http://www.informatics.jax.org/marker/MGI:1343464'
)

###############################################################################
# Using the get_keys method
# ------------------------------------------------------
# The :py:func:`~hdmf.common.resources.ExternalResources.get_keys` method
# returns a `~pandas.DataFrame` of key_name, resource_table_idx, entity_id,
# and entity_uri. You can either pass a single key object,
# a list of key objects, or leave the input paramters empty to return all.

# All Keys
er.get_keys()

# Single Key
er.get_keys(keys=er.get_key('Homo sapiens'))

# List of Specific Keys
er.get_keys(keys=[er.get_key('Homo sapiens'), er.get_key('Mus musculus')])

###############################################################################
# Using the get_key method
# ------------------------------------------------------
# The :py:func:`~hdmf.common.resources.ExternalResources.get_key`
# method will return a ``Key`` object. In the current version of ``ExternalResources``,
# duplicate keys are allowed; however, each key needs a unique linking Object.
# In other words, each combination of (container, field, key) can exist only once in
# ``ExternalResources``.

# The get_key method will return the key object of the unique (key, container, field).
key_object = er.get_key(key_name='Rorb', container=genotypes.columns[0])

###############################################################################
# Using the add_ref method with a key_object
# ------------------------------------------------------
# Multiple :py:class:`~hdmf.common.resources.Object` objects can use the same
# :py:class:`~hdmf.common.resources.Key`. To use an existing key when adding
# new entries into ``ExternalResources``, pass the :py:class:`~hdmf.common.resources.Key`
# object instead of the 'key_name' to the ``add_ref`` method. If a 'key_name' is used,
# a new Key will be created.

er.add_ref(
    container=genotypes.columns[0],
    key=key_object,
    resource_name='Ensembl',
    resource_uri='https://uswest.ensembl.org/index.html',
    entity_id='ENSG00000198963',
    entity_uri='https://uswest.ensembl.org/Homo_sapiens/Gene/Summary?db=core;g=ENSG00000198963'
)

# Let's use get_keys to visualize all the keys that have been added up to now
er.get_keys()

###############################################################################
# Using get_object_resources
# ------------------------------------------------------
# This method will return information regarding keys, resources, and entities for
# an ``Object``. You can pass either the ``AbstractContainer`` object or its
# object ID for the ``container`` argument, and the name of the field
# (container attribute) for the ``field`` argument.

er.get_object_resources(container=genotypes.columns[0])

###############################################################################
# Special Case: Using add_ref with compound data
# ------------------------------------------------------
# In most cases, the field is left as an empty string, but if the dataset or attribute
# is a compound data_type, then we can use the 'field' value to differentiate the
# different columns of the dataset. For example, if a dataset has a compound data_type with
# columns/fields 'x', 'y', and 'z', and each
# column/field is associated with different ontologies, then use field='x' to denote that
# 'x' is using the external reference.

# Let's create a new instance of ExternalResources.
er = ExternalResources(name='example')

data = Data(
    name='data_name',
    data=np.array(
        [('Mus musculus', 9, 81.0), ('Homo sapiens', 3, 27.0)],
        dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]
    )
)

er.add_ref(
    container=data,
    field='species',
    key='Mus musculus',
    resource_name='NCBI_Taxonomy',
    resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
    entity_id='NCBI:txid10090',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090'
)
