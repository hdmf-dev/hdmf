"""
ExternalResources
============

This is a user guide to interacting with the ``ExternalResources`` class.

"""

###############################################################################
# Introduction
# ------------
# The :py:class:`~hdmf.common.resources.ExternalResources` class provides a way
# to organize and map user terms (keys) to multiple resources and entities
# from the resources.

###############################################################################
# Creating an instance of the ExternalResources class
from hdmf.common import ExternalResources
from hdmf import Data
import pandas as pd

er = ExternalResources(name='test')

###############################################################################
# Using the add_ref method
# ------------
# add_ref will be one of the ExternalResources class methods that simplifies
# how to add new data.
# You can think of add_ref as a wrapper function.
#
# The function takes in the keyword arguments and will automatically create
# new entries into tables that don't have the inputs as existing data.
# In our example below, all of our Tables were empty before add_ref,
# so calling the method will create new rows of data in every table
# mentioned prior.
# Note: If you want to reference objects then you need to reference the id
# as a string, but if you want to reference a key then you reference
# the key object itself.

data = Data(name="species", data='homo sapien')
er.add_ref(container=data, field='', key='one', resource_name='new_name',
           resource_uri='new_uri', entity_id='111', entity_uri='entity_uri1')

# The data is stored in KeyTable, ObjectTable, EntityTable, and ResourceTable.

# This will return a pandas dataframe of the ResourceTable
er.resources.to_dataframe()

# This will return a pandas dataframe of the EntityTable
er.entities.to_dataframe()

# This will return a pandas dataframe of the KeyTable
er.keys.to_dataframe()

# This will return a pandas dataframe of the ObjectTable
er.objects.to_dataframe()

###############################################################################
# Using the add_key and the get_key methods
# ------------
# Mentioned in the note aobve, the user needs to reference a key object
# when referencing a key. Let's start by creating a new key.

# The add_key method will create a new key
er.add_key(key_name='two')

# The get_key method will return the key object of the key_name.
key_object = er.get_key(key_name='two')

# This will return a pandas dataframe of the KeyTable
er.keys.to_dataframe()

###############################################################################
# Using the add_keys method
# ------------
# This is another wrapper function, but allows the use of a pandas DataFrame
# to add/reference keys, resources, and entities.

new_data = {
    'key_name': 'two',
    'resources_idx': 1,
    'entity_id': '12345',
    'entity_uri': 'entity_uri4'}

df = pd.DataFrame(new_data, index=[0])

er.add_keys(df)

###############################################################################
# Using the get_keys method
# ------------
# This method returns a DataFrame of key_name, resource_table_idx, entity_id,
# and entity_uri. You can either have a single key object,
# a list of key objects, or leave the input paramters empty to return all.

# Single Key
er.get_keys(keys=er.get_key('one'))

# List of Specific Keys
er.get_keys(keys=[er.get_key('one'), er.get_key('two')])

# All Keys
er.get_keys()

###############################################################################
# Using the add_resource method
# ------------
# This method creates new resource objects and adds them to the ResourceTable.
# The ResourceTable holds the name and uri for the resource.
# For example, you could add NCBI Taxonomy with its uri.

er.add_resource(resource='taxonomy', uri='uri')

###############################################################################
# Using the add_entity method
# ------------
# This method creates a new entity object and adds it to the EntityTable.
# Keeping with the NCBI Taxonomy example, the entity would be a specfic item
# or "search" in NCBI Taxonomy, whereas the resource is NCBI Taxonomy itself.

er.add_entity(key='four', resources_idx=1,
              entity_id='113', entity_uri='entity_uri3')

###############################################################################
# Using the add_object method
# ------------
# This method creates a new object and adds it to the ObjectTable.

er.add_object(container=data, field='')

###############################################################################
# Using the add_external_reference method
# ------------
# This method creates objectkey to ObjectKeyTable

# This method below returns an Object object.
object_ = er._check_object_field(data, field="")

key_object = er.get_key(key_name='one')

er.add_external_reference(object_, key_object)

# This return ObjectKeyTable
er.object_keys.to_dataframe()
