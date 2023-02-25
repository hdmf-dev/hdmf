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
to organize and map user terms (keys) to multiple resources and entities
from the resources. A typical use case for external resources is to link data
stored in datasets or attributes to ontologies. For example, you may have a
dataset ``country`` storing locations. Using
:py:class:`~hdmf.common.resources.ExternalResources` allows us to link the
country names stored in the dataset to an ontology of all countries, enabling
more rigid standardization of the data and facilitating data query and
introspection.

From a user's perspective, one can think of the
:py:class:`~hdmf.common.resources.ExternalResources` as a simple table, in which each
row associates a particular ``key`` stored in a particular ``object`` (i.e., Attribute
or Dataset in a file) with a particular ``entity`` (e.g., a term)  of an online
``resource`` (e.g., an ontology). That is, ``(object, key)`` refer to parts inside a
file and ``(resource, entity)`` refer to an external resource outside the file, and
:py:class:`~hdmf.common.resources.ExternalResources` allows us to link the two. To
reduce data redundancy and improve data integrity,
:py:class:`~hdmf.common.resources.ExternalResources` stores this data internally in a
collection of interlinked tables.

* :py:class:`~hdmf.common.resources.KeyTable` where each row describes a
  :py:class:`~hdmf.common.resources.Key`
* :py:class:`~hdmf.common.resources.ResourceTable` where each row describes a
  :py:class:`~hdmf.common.resources.Resource`
* :py:class:`~hdmf.common.resources.EntityTable`  where each row describes an
  :py:class:`~hdmf.common.resources.Entity`
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
   with each. I.e.,  we may have keys with the same name in different objects, but for a particular object
   all keys must be unique. This means the :py:class:`~hdmf.common.resources.KeyTable` may contain
   duplicate entries, but the :py:class:`~hdmf.common.resources.ObjectKeyTable` then must not assign
   duplicate keys to the same object.
2. In order to query specific records, the :py:class:`~hdmf.common.resources.ExternalResources` class
   uses '(object_id, relative_path, field, Key)' as the unique identifier.
3. :py:class:`~hdmf.common.resources.Object` can have multiple :py:class:`~hdmf.common.resources.Key`
   objects.
4. Multiple :py:class:`~hdmf.common.resources.Object` objects can use the same :py:class:`~hdmf.common.resources.Key`.
   Note that the :py:class:`~hdmf.common.resources.Key` may already be associated with resources
   and entities.
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
"""
######################################################
# Creating an instance of the ExternalResources class
# ----------------------------------------------------

# sphinx_gallery_thumbnail_path = 'figures/gallery_thumbnail_externalresources.png'
from hdmf.common import ExternalResources
from hdmf.common import DynamicTable
from hdmf import Data
import numpy as np
# Ignore experimental feature warnings in the tutorial to improve rendering
import warnings
warnings.filterwarnings("ignore", category=UserWarning, message="ExternalResources is experimental*")

er = ExternalResources(name='example')

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
    container=data,
    key='Homo sapiens',
    resource_name='NCBI_Taxonomy',
    resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
    entity_id='NCBI:txid9606',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606'
)

key, resource, entity = er.add_ref(
    container=data,
    key='Mus musculus',
    resource_name='NCBI_Taxonomy',
    resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
    entity_id='NCBI:txid10090',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090'
)

# Print result from the last add_ref call
print(key)
print(resource)
print(entity)

###############################################################################
# Using the add_ref method with get_resource
# -------------------------------------------
# When adding references to resources, you may want to refer to multiple entities
# within the same resource. Resource names are unique, so if you call
# :py:func:`~hdmf.common.resources.ExternalResources.add_ref` with the name of an
# existing resource, then that resource will be reused. You can also use the
# :py:func:`~hdmf.common.resources.ExternalResources.get_resource`
# method to get the :py:class:`~hdmf.common.resources.Resource` object and pass that in
# to :py:func:`~hdmf.common.resources.ExternalResources.add_ref` to reuse an existing
# resource.

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
# Using the add_ref method with get_resource
# -------------------------------------------
# When adding references to resources, you may want to refer to multiple entities
# within the same resource. Resource names are unique, so if you call
# :py:func:`~hdmf.common.resources.ExternalResources.add_ref` with the name of an
# existing resource, then that resource will be reused. You can also use the
# :py:func:`~hdmf.common.resources.ExternalResources.get_resource`
# method to get the :py:class:`~hdmf.common.resources.Resource` object and pass that in
# to :py:func:`~hdmf.common.resources.ExternalResources.add_ref` to reuse an existing
# resource.

# Let's create a new instance of ExternalResources.
er = ExternalResources(name='example')

data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
er.add_ref(
    container=data,
    field='',
    key='Homo sapiens',
    resource_name='NCBI_Taxonomy',
    resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
    entity_id='NCBI:txid9606',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606')

# Using get_resource
existing_resource = er.get_resource('NCBI_Taxonomy')
er.add_ref(
    container=data,
    field='',
    key='Mus musculus',
    resources_idx=existing_resource,
    entity_id='NCBI:txid10090',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')

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
#
# Note: :py:func:`~hdmf.common.resources.ExternalResources.add_ref` internally resolves the object
# to the closest parent, so that ``er.add_ref(container=genotypes, attribute='genotype_name')`` and
# ``er.add_ref(container=genotypes.genotype_name, attribute=None)`` will ultimately both use the ``object_id``
# of the ``genotypes.genotype_name`` :py:class:`~hdmf.common.table.VectorData` column and
# not the object_id of the genotypes table.

genotypes = DynamicTable(name='genotypes', description='My genotypes')
genotypes.add_column(name='genotype_name', description="Name of genotypes")
genotypes.add_row(id=0, genotype_name='Rorb')
er.add_ref(
    container=genotypes,
    attribute='genotype_name',
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
# returns a :py:class:`~pandas.DataFrame` of ``key_name``, ``resource_table_idx``, ``entity_id``,
# and ``entity_uri``. You can either pass a single key object,
# a list of key objects, or leave the input parameters empty to return all.

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
# method will return a :py:class:`~hdmf.common.resources.Key` object. In the current version of
# :py:class:`~hdmf.common.resources.ExternalResources`, duplicate keys are allowed; however, each key needs a unique
# linking Object. In other words, each combination of (container, relative_path, field, key) can exist only once in
# :py:class:`~hdmf.common.resources.ExternalResources`.

# The get_key method will return the key object of the unique (key, container, relative_path, field).
key_object = er.get_key(key_name='Rorb', container=genotypes.columns[0])

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
    container=genotypes,
    attribute='genotype_name',
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
# ---------------------------
# This method will return information regarding keys, resources, and entities for
# an :py:class:`~hdmf.common.resources.Object`. You can pass either the ``AbstractContainer`` object or its
# object ID for the ``container`` argument, and the corresponding relative_path and field.

er.get_object_resources(container=genotypes.columns[0])

###############################################################################
# Special Case: Using add_ref with compound data
# ------------------------------------------------
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

###############################################################################
# Note that because the container is a :py:class:`~hdmf.container.Data` object, and the external resource is being
# associated with the values of the dataset rather than an attribute of the dataset,
# the field must be prefixed with 'data'. Normally, to associate an external resource
# with the values of the dataset, the field can be left blank. This allows us to
# differentiate between a dataset compound data type field named 'x' and a dataset
# attribute named 'x'.

er.add_ref(
    container=data,
    field='species',
    key='Homo sapiens',
    resource_name='NCBI_Taxonomy',
    resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
    entity_id='NCBI:txid9606',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606'
)


###############################################################################
# Convert ExternalResources to a single DataFrame
# -----------------------------------------------
#

er = ExternalResources(name='example')

data1 = Data(
    name='data_name',
    data=np.array(
        [('Mus musculus', 9, 81.0), ('Homo sapiens', 3, 27.0)],
        dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]
    )
)

k1, r1, e1 = er.add_ref(
    container=data1,
    field='species',
    key='Mus musculus',
    resource_name='NCBI_Taxonomy',
    resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
    entity_id='NCBI:txid10090',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090'
)


k2, r2, e2 = er.add_ref(
    container=data1,
    field='species',
    key='Homo sapiens',
    resource_name='NCBI_Taxonomy',
    resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
    entity_id='NCBI:txid9606',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606'
)

# Want to use the same key, resources, and entities for both. But we'll add an extra key just for this one
data2 = Data(name="species", data=['Homo sapiens', 'Mus musculus', 'Pongo abelii'])

o2 = er._add_object(data2, relative_path='', field='')
er._add_object_key(o2, k1)
er._add_object_key(o2, k2)

k2, r2, e2 = er.add_ref(
    container=data2,
    field='',
    key='Pongo abelii',
    resource_name='NCBI_Taxonomy',
    resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
    entity_id='NCBI:txid9601',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9601'
)

# Question:
# - Can add_ref be used to associate two different objects with the same keys, resources, and entities?
#    - Here we use the private _add_object, and _add_object_key methods to do this but should this not be possible
#      with add_ref? Specifically, add_ref allows Resource, Key, objects to be reused on input but not Entity? Why?
#      E.g., should we be able to do:
#      er.add_ref(
#         container=data2,
#         field='',
#         key=k1,
#         resources_idx=r1,
#         entity_id=e1      # <-- not allowed
#      )
#

genotypes = DynamicTable(name='genotypes', description='My genotypes')
genotypes.add_column(name='genotype_name', description="Name of genotypes")
genotypes.add_row(id=0, genotype_name='Rorb')
k3, r3, e3 = er.add_ref(
    container=genotypes['genotype_name'],
    field='',
    key='Rorb',
    resource_name='MGI Database',
    resource_uri='http://www.informatics.jax.org/',
    entity_id='MGI:1346434',
    entity_uri='http://www.informatics.jax.org/marker/MGI:1343464'
)
er.add_ref(
    container=genotypes['genotype_name'],
    field='',
    key=k3,
    resource_name='Ensembl',
    resource_uri='https://uswest.ensembl.org/index.html',
    entity_id='ENSG00000198963',
    entity_uri='https://uswest.ensembl.org/Homo_sapiens/Gene/Summary?db=core;g=ENSG00000198963'
)


###############################################################################
# Convert the individual tables to DataFrames
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

er.keys.to_dataframe()
###############################################################################
#
er.resources.to_dataframe()
###############################################################################
# Note that key 3 has 2 entities assigned to it in the entities table
er.entities.to_dataframe()
###############################################################################
#
er.objects.to_dataframe()
###############################################################################
# Note that key 0 and 1 are used by both object 0 and object 1 in the object_keys table
er.object_keys.to_dataframe()
###############################################################################
# Convert the whole ExternalResources to a single DataFrame
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# Using the :py:class:`~hdmf.common.resources.ExternalResources.to_dataframe` method of the
# :py:class:`~hdmf.common.resources.ExternalResources` we can convert the data from the corresponding
# :py:class:`~hdmf.common.resources.Keys`, :py:class:`~hdmf.common.resources.Resources`,
# :py:class:`~hdmf.common.resources.Entities`, :py:class:`~hdmf.common.resources.Objects`, and
# :py:class:`~hdmf.common.resources.ObjectKeys` tables to a single joint :py:class:`~pandas.DataFrame`.
# In this conversion the data is being denormalized, such that e.g.,
# the :py:class:`~hdmf.common.resources.Keys` that are used across multiple :py:class:`~hdmf.common.resources.Enitites`
# are duplicated across the corresponding rows. Here this is the case, e.g., for the keys ``"Homo sapiens"`` and
# ``"Mus musculus"`` which are used in the first two objects (rows with ``index=[0, 1, 2, 3]``), or the
# ``Rorb`` key which appears in both the ``MGI Database`` and  ``Ensembl`` resource (rows with ``index=[5,6]``).
er.to_dataframe()

###############################################################################
# By setting ``use_categories=True`` the function will use a :py:class:`pandas.MultiIndex` on the columns
# instead to indicate for each column also the category (i.e., ``objects``, ``keys``, ``entities``, and ``resources``
# the columns belong to. **Note:** The category in the combined table is not the same as the name of the source table
# but rather represents the semantic category, e.g., ``keys_idx`` appears as a foreign key in both the
# :py:class:`~hdmf.common.resources.ObjectKeys` and :py:class:`~hdmf.common.resources.Entities` tables
# but in terms of the combined table is a logical property of the ``keys``.
er.to_dataframe(use_categories=True)

###############################################################################
# Export ExternalResources to SQLite
# ----------------------------------

# Set the database file to use and clean up the file if it exists
import os
db_file = "test_externalresources.sqlite"
if os.path.exists(db_file):
    os.remove(db_file)

###############################################################################
# Export the data stored in the :py:class:`~hdmf.common.resources.ExternalResources`
# object to a SQLite database.
er.to_sqlite(db_file)

###############################################################################
# Test that the generated SQLite database is correct

import sqlite3
import pandas as pd
from contextlib import closing

with closing(sqlite3.connect(db_file)) as db:
    cursor = db.cursor()
    # read all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    # convert all tables to pandas and compare with the original tables
    for table_name in tables:
        table_name = table_name[0]
        table = pd.read_sql_query("SELECT * from %s" % table_name, db)
        table = table.set_index('id')
        ref_table = getattr(er, table_name).to_dataframe()
        assert np.all(np.array(table.index) == np.array(ref_table.index) + 1)
        for c in table.columns:
            # NOTE: SQLite uses 1-based row-indices so we need adjust for that
            if np.issubdtype(table[c].dtype, np.integer):
                assert np.all(np.array(table[c]) == np.array(ref_table[c]) + 1)
            else:
                assert np.all(np.array(table[c]) == np.array(ref_table[c]))
    cursor.close()

###############################################################################
# Remove the test file
os.remove(db_file)
