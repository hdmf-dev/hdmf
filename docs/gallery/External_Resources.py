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
# from the resources.

###############################################################################
# Creating an instance of the ExternalResources class
from hdmf.common import ExternalResources
from hdmf.common import DynamicTable
from hdmf import Data

er = ExternalResources(name='example')

###############################################################################
# Using the add_ref method
# ------------------------------------------------------
# add_ref takes inputs of new data via keyword arguments
# and adds to ExternalResources.
# You can think of add_ref as a wrapper function.

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
# We can use the get_resource method to reference an existing resource within
# ExternalResources when adding data with add_ref.

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
# In the above example, field was empty because the data of the Data object was
# being associated with a resource. The Data object may also have attributes
# that you would like to associate with resources.
# Similarly, a Container object may have several datasets and attributes that
# you would like to associate with resources.
# To disambiguate between these different fields, you can set the 'field' keyword
# argument corresponding to the name of the dataset or attribute containing the
# values being associated with a resource. For example:

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
# This method will return a key_object. In the current version of ExternalResources
# duplicate keys are allowed; however, each key needs a unique linking Object.
# i.e (container, field)

# The get_key method will return the key object of the unique (key, container, field).
key_object = er.get_key(key_name='Rorb', container=genotypes, field='genotype_name')

###############################################################################
# Using the add_ref method with a key_object
# ------------------------------------------------------
# Sometimes you want to reference a specific key that already exists when adding
# new ontology data into ExternalResources.

er.add_ref(container=genotypes, field='genotype_name', key=key_object, resource_name='Ensembl',
           resource_uri='https://uswest.ensembl.org/index.html', entity_id='ENSG00000198963',
           entity_uri='https://uswest.ensembl.org/Homo_sapiens/Gene/Summary?db=core;g=ENSG00000198963')

# Let's use get_keys to visualize
er.get_keys()
