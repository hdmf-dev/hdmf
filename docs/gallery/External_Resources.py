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
# dataset `country` storing locations. Using
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
#  :py:class:`~hdmf.common.resources.ObjectKey` pair identifying which keys
#   are used by which objects.
#
# The :py:class:`~hdmf.common.resources.ExternalResources` class then provides
# convenience functions to simplify interaction with these tables, allowing users
# to treat ``ExternalResources`` as a single large table as much as possible.

###############################################################################
# Creating an instance of the ExternalResources class
from hdmf.common import ExternalResources
from hdmf.common import DynamicTable
from hdmf import Data
import numpy as np

er = ExternalResources(name='example')

###############################################################################
# Using the add_ref method
# ------------------------------------------------------
# :py:func:`~hdmf.common.resources.ExternalResources.add_ref`
# is a wrapper function provided by the ``ExternalResources`` class, that
# simplifies adding data. Using ``add_ref`` allows us to treat new entries similar
# to adding a new row to a flat table, with ``add_ref`` taking care of populating
# the underlying data structures accordingly.

data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
er.add_ref(container=data, field='', key='Homo sapiens', resource_name='NCBI_Taxonomy',
           resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy', entity_id='NCBI:txid9606',
           entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606')

er.add_ref(container=data, field='', key='Mus musculus', resource_name='NCBI_Taxonomy',
           resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy', entity_id='NCBI:txid10090',
           entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')

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
er.add_ref(container=data, field='', key='Homo sapiens', resource_name='NCBI_Taxonomy',
           resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy', entity_id='NCBI:txid9606',
           entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606')

# Using get_resource
existing_resource = er.get_resource('NCBI_Taxonomy')
er.add_ref(container=data, field='', key='Mus musculus', resources_idx=existing_resource,
           entity_id='NCBI:txid10090',
           entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')

###############################################################################
# Using the add_ref method with a field
# ------------------------------------------------------
# In the above example, the ``field`` keyword argument was empty because the data
# of the :py:class:`~hdmf.container.Data` object passed in for the ``container``
# argument was being associated with a resource. However, you may want to associate
# an attribute of a :py:class:`~hdmf.container.Data` object with a resource or
# a dataset or attribute of a :py:class:`~hdmf.container.Container` object with
# a resource. To disambiguate between these different fields, you can set the
# 'field' keyword.

genotypes = DynamicTable(name='genotypes', description='My genotypes')
genotypes.add_column(name='genotype_name', description="Name of genotypes")
genotypes.add_row(id=0, genotype_name='Rorb')
er.add_ref(container=genotypes, field='genotype_name', key='Rorb', resource_name='MGI Ontology',
           resource_uri='http://www.informatics.jax.org/', entity_id='MGI:1346434',
           entity_uri="http://www.informatics.jax.org/probe/key/804614")

###############################################################################
# Using the get_keys method
# ------------------------------------------------------
# This method returns a DataFrame of key_name, resource_table_idx, entity_id,
# and entity_uri. You can either have a single key object,
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
# This method will return a ``Key`` object. In the current version of ``ExternalResources``,
# duplicate keys are allowed; however, each key needs a unique linking Object.
# In other words, each combination of (container, field, key) can exist only once in
# ``ExternalResources``.

# The get_key method will return the key object of the unique (key, container, field).
key_object = er.get_key(key_name='Rorb', container=genotypes, field='genotype_name')

###############################################################################
# Using the add_ref method with a key_object
# ------------------------------------------------------
# Sometimes you want to reference a specific key that already exists when adding
# new ontology data into ``ExternalResources``.

er.add_ref(container=genotypes, field='genotype_name', key=key_object, resource_name='Ensembl',
           resource_uri='https://uswest.ensembl.org/index.html', entity_id='ENSG00000198963',
           entity_uri='https://uswest.ensembl.org/Homo_sapiens/Gene/Summary?db=core;g=ENSG00000198963')

# Let's use get_keys to visualize
er.get_keys()

###############################################################################
# Special Case: Using add_ref with multi-level fields
# ------------------------------------------------------
# In most cases, the field is the name of a dataset or attribute,
# but it could be a little more complicated. Let's say the attribute is not a string
# but a compound data type with columns/fields 'x', 'y', and 'z', and each
# column/field is associated with different ontologies. The 'field' value also needs
# to account for this. This should done using '/' as a separator, e.g.,
# field='data/unit/x'.

# Let's create a new instance of ExternalResources.
er = ExternalResources(name='example')

data = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
            dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))

er.add_ref(container=data, field='data/species', key='Mus musculus', resource_name='NCBI_Taxonomy',
           resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
           entity_id='NCBI:txid10090',
           entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')

er.add_ref(container=data, field='data/species', key='Homo sapiens', resource_name='NCBI_Taxonomy',
           resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
           entity_id='NCBI:txid9606',
           entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606')
