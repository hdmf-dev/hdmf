"""
TermSet
=======

This is a user guide for interacting with the
:py:class:`~hdmf.term_set.TermSet` class. The :py:class:`~hdmf.term_set.TermSet` type
is experimental and is subject to change in future releases. If you use this type,
please provide feedback to the HDMF team so that we can improve the structure and
overall capabilities.

Introduction
-------------
The :py:class:`~hdmf.term_set.TermSet` class provides a way for users to create their own
set of terms from brain atlases, species taxonomies, and anatomical, cell, and
gene function ontologies.

:py:class:`~hdmf.term_set.TermSet` serves two purposes: data validation and external reference
management. Users will be able to validate their data to their own set of terms, ensuring
clean data to be used inline with the FAIR principles later on.
The  :py:class:`~hdmf.term_set.TermSet` class allows for a reusable and sharable
pool of metadata to serve as references to any dataset.
The :py:class:`~hdmf.term_set.TermSet` class is used closely with
:py:class:`~hdmf.common.resources.ExternalResources` to more efficiently map terms
to data. Please refer to the tutorial on ExternalResources to see how :py:class:`~hdmf.term_set.TermSet`
is used with :py:class:`~hdmf.common.resources.ExternalResources`.

:py:class:`~hdmf.term_set.TermSet` is built upon the resources from LinkML, a modeling
language that uses YAML-based schema, giving :py:class:`~hdmf.term_set.TermSet`
a standardized structure and a variety of tools to help the user manage their references.

How to make a TermSet Schema
----------------------------
Before the user can take advantage of all the wonders within the
:py:class:`~hdmf.term_set.TermSet` class, the user needs to create a LinkML schema (YAML) that provides
all the permissible term values. Please refer to https://linkml.io/linkml/intro/tutorial06.html
to learn more about how LinkML structures their schema.

1. The name of the schema is up to the user, e.g., the name could be "Species" if the term set will
   contain species terms.
2. The prefixes will be the standardized prefix of your source, followed by the URI to the terms.
   For example, the NCBI Taxonomy is abbreviated as NCBI_TAXON, and Ensemble is simply Ensemble.
   As mentioned prior, the URI needs to be to the terms; this is to allow the URI to later be coupled
   with the source id for the term to create a valid link to the term source page.
3. The schema uses LinkML enumerations to list all the possible terms. Currently, users will need to
   manually outline the terms within the enumeration's permissible values.

For a clear example, please view the
`example_term_set.yaml <https://github.com/hdmf-dev/hdmf/blob/dev/docs/gallery/example_term_set.yaml>`_
for this tutorial, which provides a concise example of how a term set schema looks.
"""
######################################################
# Creating an instance of the TermSet class
# ----------------------------------------------------
from hdmf.common import DynamicTable, VectorData
import os

try:
    dir_path = os.path.dirname(os.path.abspath(__file__))
    yaml_file = os.path.join(dir_path, 'example_term_set.yaml')
except NameError:
    dir_path = os.path.dirname(os.path.abspath('.'))
    yaml_file = os.path.join(dir_path, 'gallery/example_term_set.yaml')

######################################################
# Viewing TermSet values
# ----------------------------------------------------
# :py:class:`~hdmf.term_set.TermSet` has methods to retrieve terms. The :py:func:`~hdmf.term_set.TermSet:view_set`
# method will return a dictionary of all the terms and the corresponding information for each term.
# Users can index specific terms from the :py:class:`~hdmf.term_set.TermSet`. LinkML runtime will need to be installed.
# You can do so by first running ``pip install linkml-runtime``.
from hdmf.term_set import TermSet
terms = TermSet(term_schema_path=yaml_file)
print(terms.view_set)

# Retrieve a specific term
terms['Homo sapiens']

######################################################
# Validate Data with TermSet
# ----------------------------------------------------
# :py:class:`~hdmf.term_set.TermSet` has been integrated so that :py:class:`~hdmf.container.Data` and its
# subclasses support a term_set attribute. By having this attribute set, the data will be validated
# and all new data will be validated.
data = VectorData(
    name='species',
    description='...',
    data=['Homo sapiens'],
    term_set=terms)

######################################################
# Validate on append with TermSet
# ----------------------------------------------------
# As mentioned prior, when the term_set attribute is set, then all new data is validated. This is true for both
# append and extend methods.
data.append('Ursus arctos horribilis')
data.extend(['Mus musculus', 'Myrmecophaga tridactyla'])

######################################################
# Validate Data in a DynamicTable with TermSet
# ----------------------------------------------------
# Validating data with :py:class:`~hdmf.common.table.DynamicTable` is determined by which columns were
# initialized with the term_set attribute set. The data is validated when the columns are created or
# modified. Since adding the columns to a DynamicTable does not modify the data, validation is
# not being performed at that time.
col1 = VectorData(
    name='Species_1',
    description='...',
    data=['Homo sapiens'],
    term_set=terms,
)
col2 = VectorData(
    name='Species_2',
    description='...',
    data=['Mus musculus'],
    term_set=terms,
)
species = DynamicTable(name='species', description='My species', columns=[col1,col2])

######################################################
# Validate new rows in a DynamicTable with TermSet
# ----------------------------------------------------
# Validating new rows to :py:class:`~hdmf.common.table.DynamicTable` is simple. The
# :py:func:`~hdmf.common.table.DynamicTable.add_row` method will automatically check each column for a
# :py:class:`~hdmf.term_set.TermSet` (via the term_set attribute). If the attribute is set, the the data will be
# validated for that column using that column's :py:class:`~hdmf.term_set.TermSet`. If there is invalid data, the
# row will not be added and the user will be prompted to fix the new data in order to populate the table.
species.add_row(Species_1='Mus musculus', Species_2='Mus musculus')

######################################################
# Validate new columns in a DynamicTable with TermSet
# ----------------------------------------------------
# As mentioned prior, validating in a :py:class:`~hdmf.common.table.DynamicTable` is determined
# by the columns. The :py:func:`~hdmf.common.table.DynamicTable.add_column` method has a term_set attribute
# as if you were making a new instance of :py:class:`~hdmf.common.table.VectorData`. When set, this attribute
# will be used to validate the data. The column will not be added if there is invalid data.
col1 = VectorData(
    name='Species_1',
    description='...',
    data=['Homo sapiens'],
    term_set=terms,
)
species = DynamicTable(name='species', description='My species', columns=[col1])
species.add_column(name='Species_2',
                   description='Species data',
                   data=['Mus musculus'],
                   term_set=terms)
