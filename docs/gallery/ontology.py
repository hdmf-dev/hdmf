"""
Ontology
=============================

This is a user guide for interacting with the assorted ``Ontology`` classes, all
of which are experimental and are subject to change in future releases.
Please provide feedback to the HDMF team so that we can improve the structure
and usability for your use cases.

"""

###############################################################################
# Introduction
# ------------------------------------------------------
# With the :py:class:`~hdmf.ontology.Ontology` class and the subsequent subclasses
# that inherit from :py:class:`~hdmf.ontology.Ontology`, users can more easily
# link ontologies and ontology entities to
# :py:class:`~hdmf.common.resources.ExternalResources`.
# Fully integrated with the :py:class:`~hdmf.container.Container` class, users
# can add ontologies through two approaches. Firstly, users can use the
# :py:func:`~hdmf.container.Container.add_ontology` method within the
# :py:class:`~hdmf.container.Container` class to add an ontology as a
# :py:class:`~hdmf.common.resources.Resource and add an
# ontology entity as an :py:class:`~hdmf.common.resources.Entity' to the
# :py:class:`~hdmf.common.resources.ExternalResources` within the NWBFILE.
# Lastly, users are able to link ontologies to a :py:class:`~hdmf.container.Container`
# without immediatly adding to :py:class:`~hdmf.common.resources.ExternalResources`.
# A typical use case of the latter would be to create a controlled set of values
# within data. The user is able to add this "linked" ontology to
# :py:class:`~hdmf.common.resources.ExternalResources` via (TBD on function).

from hdmf import Ontology, EnsemblOntology, NCBI_Taxonomy, WebAPIOntology, LocalOntology, Container
from ndx_external_resources import ERNWBFile
from ndx_genotype import GenotypeSubject, GenotypesTable, AllelesTable
import datetime
from dateutil.tz import tzlocal


###############################################################################
# Creating a LocalOntology
# ------------------------------------------------------
# Users are able to create their own ontologies that will be stored locally on
# their machine with :py:class:`~hdmf.ontology.LocalOntology`. Users will define
# the entities of the ontology within a python dictionary. To add new entries,
# it is required that the entries have both the entity id and the corresponding uri.
# Once added, users can add or remove entries with built-in methods.
# :py:class:`~hdmf.ontology.LocalOntology` also supports the ability to write
# and read the ontology to and from a YAML file.

# Let's create an instance of :py:class:`~hdmf.ontology.LocalOntology`.
ontology = LocalOntology(version='1.0', ontology_name='ontology_name', ontology_uri='ontology_uri')

# Populating the ontology
ontology.add_ontology_entity(key='Homo sapiens', entity_value=['9606', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606'])
# named tuple/dict to provide context of the list values

# Removing an entry from the ontology
ontology.remove_ontology_entity('Homo sapiens')

# Note: You can set the ontology entities when creating a new instance of :py:class:`~hdmf.ontology.LocalOntology`.
ontology_entities={"Homo sapiens": ['9606', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606']}
ontology = LocalOntology(version='1.0', ontology_name='ontology_name', ontology_uri='ontology_uri', ontology_entities=ontology_entities)

# Writing the ontology as an YAML file
ontology.write_ontology_yaml(path="path_to_target_directory")

# Reading a YAML file to python dictionary
ontology_dict = ontology.read_ontology_yaml(path="path_to_target_directory")

# Retrieving specfic entries from the ontology with the entity key
ontology.get_ontology_entity('Homo sapiens')


###############################################################################
# Creating a WebAPIOntology
# ------------------------------------------------------
# Some ontologies have a Web API to faciliate users in retrieving specfic entries.
# The :py:class:`~hdmf.ontology.WebAPIOntology` inherits from :py:class:`~hdmf.ontology.LocalOntology`.
# We've given the :py:func:`~hdmf.ontology.WebAPIOntology.get_ontology_entity`
# method the ability to request and retrieve ontology entries from a Web API,
# returning the entity id and the uri for the entry. Users can add this information
# to their ontology. :py:func:`~hdmf.ontology.WebAPIOntology.get_ontology_entity`
# also supports retrieving ontology entities within the instance exactly the same
# as in :py:class:`~hdmf.ontology.LocalOntology`.

# Let's create an instance of :py:class:`~hdmf.ontology.LocalOntology`.
ontology = WebAPIOntology(version='1.0', ontology_name='Ensembl', ontology_uri='https://rest.ensembl.org', extension='/taxonomy/id/')

# Using :py:func:`~hdmf.ontology.WebAPIOntology.get_ontology_entity` to request Web API
entity_id, entity_uri = ontology.get_ontology_entity(key='Homo sapiens')

# Populating the ontology
ontology.add_ontology_entity(key='Homo sapiens', entity_value=[entity_id, entity_uri])

###############################################################################
# The :py:class:`~hdmf.ontology.EnsemblOntology` class
# ------------------------------------------------------
# Ensembl is a popular ontology that offers a Web API.
# The :py:class:`~hdmf.ontology.EnsemblOntology` inherits from
# :py:class:`~hdmf.ontology.WebAPIOntology`; the difference being preset ontology
# name and uri for Ensembl.

# Let's create an instance of :py:class:`~hdmf.ontology.Ensembl`.
ontology = EnsemblOntology(version='1.0')


###############################################################################
# The :py:class:`~hdmf.ontology.NCBI_Taxonomy` class
# ------------------------------------------------------
# The NCBI Taxonomy is a popular species ontology. Due to its common use, the
# :py:class:`~hdmf.ontology.NCBI_Taxonomy` class offers preset ontology
# name and uri for the NCBI Taxonomy.

# Let's create an instance of :py:class:`~hdmf.ontology.NCBI_Taxonomy`.
ontology = NCBI_Taxonomy(version='1.0')

###############################################################################
# Using :py:func:`~hdmf.container.Container.add_ontology_resource` method within the
# :py:class:`~hdmf.container.Container` class
# ------------------------------------------------------
# The :py:func:`~hdmf.container.Container.add_ontology_resource` method within the
# :py:class:`~hdmf.container.Container` class will add an ontology to the
# :py:class:`~hdmf.common.resources.ExternalResources` within the NWBFILE. The method
# follows the same input parameters as :py:func:`~hdmf.common.resources.ExternalResources.add_ref`.
# The ontology instance will be the :py:class:`~hdmf.common.resources.Resource
# and the ontology entry will be the :py:class:`~hdmf.common.resources.Entity'.
# The :py:class:`~hdmf.container.Container` will be used to provide the
# necessary object id. The :py:func:`~hdmf.container.Container.add_ontology` method
# supports adding the ontology entries in bulk by providing the desired ontology
# keys to be added via a list. The method will only add valid keys. It will return
# both the valid keys added and the invalid keys to be reviewed as lists.

nwbfile = ERNWBFile(
            session_description='session_description',
            identifier='identifier',
            session_start_time=datetime.datetime.now(datetime.timezone.utc)
        )
nwbfile.subject = GenotypeSubject(
            subject_id='3',
            genotype='Vip-IRES-Cre/wt',
            species='Homo sapiens'
        )

ontology = EnsemblOntology(version='1.0')
nwbfile.subject.add_ontology_resource(key=['Homo sapiens'], attribute='species', ontology=ontology)

###############################################################################
# Using Ontologies to control vocavulary in :py:class:`~hdmf.container.Data`
# ------------------------------------------------------
# Users have the ability to use ontologies to control the dataset values within
# :py:class:`~hdmf.container.Data`. Users can pass in an ontology as an optional
# parameter. Passing in an ontology must be done on the creation of a new instance
# :py:class:`~hdmf.container.Data`, in which the the new :py:class:`~hdmf.container.Data`
# object is created only if the data values pass validation with current entries
# within the ontology. Users cannot add an ontology to an existing instance of
# :py:class:`~hdmf.container.Data` to retoactively control data values. If a user
# wants to add to :py:class:`~hdmf.common.resources.ExternalResources` using their data
# and ontology, then use the :py:func:`~hdmf.container.Container.add_ontology_resource` method
# in the following example.

ontology_obj = WebAPIOntology(version='1.0', ontology_name='Ensembl', ontology_uri='https://rest.ensembl.org', extension='/taxonomy/id/', _ontology_entities=TestData._ontology_entities)
data_obj = Data(name='name', data =['Homo sapiens'], ontology=ontology_obj)
