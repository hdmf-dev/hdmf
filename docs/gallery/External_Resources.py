"""
ExternalResources
=============================

This is a user guide to interacting with the 
:py:class:`~hdmf.common.resources.ExternalResources` class as part of the
:py:module:`~hdmf.common.resources` module.

.. note::
   The ExternalResources type is experimental and is subject to change in future 
   releases. If you use this type, please provide feedback to the HDMF team so 
   that we can improve the structure and access of data stored with this type for
   your use cases.

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
# From a users perspective, one can think of the ``ExternalResources`` as a 
# simple table, in which each row associates a particular ``key`` stored in a
# particular ``object`` (i.e., Attribute or Dataset in a file) with a particular 
# ``entity`` (e.g., a term)  of an online ``resource`` (e.g., an ontology). 
# That is, ``(object, key)`` refer to parts inside a file and ``(resource, entity)``
# refer to an external resource outside of the file, and ``ExternalResources`` 
# allows us to link the two. To reduce data redundancy and improve data integrity 
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
# convenienve functions to simply interation with these tables, allowing users
# to treat ``ExternalResources`` as a single large table as much as possible. 

###############################################################################
# Creating an instance of the ExternalResources class
from hdmf.common import ExternalResources
from hdmf.common import DynamicTable
from hdmf import Data
import pandas as pd

er = ExternalResources(name='example')

###############################################################################
# Using the add_ref method
# ------------------------------------------------------
# :py:func:`~hdmf.common.resources.ExternalResources.add_ref`
# is a wrapper function provided by the ExternalResources class, that 
# simplifies adding data. Using add_ref allows us to treat new entries similar 
# to adding a new row to a flat table, with add_ref taking care of populating 
# the underlying data structures accordingly.
#
# add_ref takes in a set of the keyword arguments and automatically creates
# new entries into the approbriate tables that underly ExternalResources as
# necessary. In our example below, all of our tables were empty before add_ref,
# so calling the method will create new rows of data in every table
# mentioned prior.
# 
# .. note:: 
#     If you want to reference objects then you need to reference the id
#     as a string, but if you want to reference a key then you reference
#     the :py:class:`~hdmf.common.resources.Key` object itself. We can retrive
#     keys via the :py:func:`~hdmf.common.resources.ExternalResources.get_key`
#     described below.

data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
er.add_ref(container=data, field='', key='Homo sapiens', resource_name='NCBI_Taxonomy',
           resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy', entity_id='NCBI:txid9606',
           entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606')

er.add_ref(container=data, field='', key='Mus musculus', resource_name='NCBI_Taxonomy',
           resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy', entity_id='NCBI:txid10090',
           entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')

###############################################################################
# Using the add_ref method with a field
# ------------------------------------------------------
# In the above example, ``field`` was empty because the data of the ``container`` 
# data object itself was being associated with a resource. However, rather than
# the ``container`` data object itself, you may instead want to associated external 
# resources with datasets or attributes contained in the data object. 
# that you would like to associate with resources. To disambiguate between 
# these different fields, you can set the 'field' keyword
# argument corresponding to the name of the dataset or attribute containing the
# values being associated with a resource. For example:
genotypes = DynamicTable(name='genotypes', description='My genotypes')
genotypes.add_column(name='genotype_name', description="Name of genotypes")
genotypes.add_row(id=0, genotype_name='Rorb')
er.add_ref(container=genotypes, field='genotype_name', key='Mus musculus', resource_name='MGI Ontology',
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
# Using the add_key and the get_key methods
# ------------------------------------------------------
# Mentioned in the note above, the user needs to reference a key object
# when referencing a key. Let's start by creating a new key.

# Let's start with a new instance of ExternalResources
er = ExternalResources(name='new_example')

# The add_key method will create a new key
er.add_key(key_name='Homo sapiens')
er.add_key(key_name='Mus musculus')

# The get_key method will return the key object of the key_name.
key_object = er.get_key(key_name='Mus musculus')

# We can use the key_object to reference keys.
er.add_ref(container=data, field='', key=key_object, resource_name='NCBI_Taxonomy',
           resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy', entity_id='NCBI:txid10090',
           entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')

# It is also useful to associate an existing key with an additional entity.
# For example, we can reference the Mus musculus key for a different entity from
# a new resource.
er.add_ref(container=data, field='', key=key_object, resource_name='ITIS',
           resource_uri='https://www.itis.gov/', entity_id='180366',
           entity_uri='https://www.itis.gov/servlet/SingleRpt/SingleRpt?search_topic=TSN&search_value=180366')

er.get_keys()

# This will return a pandas dataframe of the KeyTable
er.keys.to_dataframe()

###############################################################################
# Using the add_keys method
# ------------------------------------------------------
# This is another wrapper function, but allows the use of a pandas DataFrame
# to add/reference keys, resources, and entities.

new_data = {
    'key_name': 'Homo sapiens',
    'resources_idx': 0,
    'entity_id': 'NCBI:txid9606',
    'entity_uri': 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606'}

df = pd.DataFrame(new_data, index=[0])

er.add_keys(df)

###############################################################################
# Using the add_resource method
# ------------------------------------------------------
# This method creates new resource objects and adds them to the 
# :py:class:`~hdmf.common.resources.ResourceTable`.
# The ResourceTable holds the name and uri for the resource.
# For example, you could add NCBI Taxonomy with its uri.

er = ExternalResources(name='new_example')


er.add_resource(resource='NCBI_Taxonomy', uri='https://www.ncbi.nlm.nih.gov/taxonomy')

# This will return a pandas dataframe of the ResourceTable
er.resources.to_dataframe()

###############################################################################
# Using the add_entity method
# ------------------------------------------------------
# This method creates a new entity object and adds it to the
# :py:class:`~hdmf.common.resources.WntityTable`.
# Keeping with the NCBI Taxonomy example, the entity would be a specfic item
# or "search" in NCBI Taxonomy, whereas the resource is NCBI Taxonomy itself.

er.add_entity(key='Homo sapiens', resources_idx=0,
              entity_id='NCBI:txid9606',
              entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606')

er.add_entity(key='Mus musculus', resources_idx=0,
              entity_id='NCBI:txid10090',
              entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')

# This will return a pandas dataframe of the EntityTable
er.entities.to_dataframe()

###############################################################################
# Using the add_object method
# ------------------------------------------------------
# This method creates a new object and adds it to the 
# :py:class:`~hdmf.common.resources.ObjectTable`.

er.add_object(container=data, field='Data')

# This will return a pandas dataframe of the ObjectTable
er.objects.to_dataframe()

###############################################################################
# Using the add_external_reference method
# ------------------------------------------------------
# This method creates objectkey to 
# :py:class:`~hdmf.common.resources.ObjectKeyTable`

# Let's create a new Data instance
data_mouse = Data(name="species", data='Mus musculus')

# This method below returns an Object object.
object_ = er._check_object_field(data_mouse, field='')

key_object = er.get_key(key_name='Mus musculus')

# Create a new ObjectKey and add it to the ObjectKeyTable
object_key_ = er.add_external_reference(object_, key_object)

# This returns the ObjectKeyTable and converts it to a datafram
er.object_keys.to_dataframe()
