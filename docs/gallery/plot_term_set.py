"""
TermSet
=======

This is a user guide for interacting with the
:py:class:`~hdmf.term_set.TermSet` and :py:class:`~hdmf.term_set.TermSetWrapper` classes.
The :py:class:`~hdmf.term_set.TermSet` and :py:class:`~hdmf.term_set.TermSetWrapper` types
are experimental and are subject to change in future releases. If you use these types,
please provide feedback to the HDMF team so that we can improve the structure and
overall capabilities.

Introduction
-------------
The :py:class:`~hdmf.term_set.TermSet` class provides a way for users to create their own
set of terms from brain atlases, species taxonomies, and anatomical, cell, and
gene function ontologies.

Users will be able to validate their data and attributes to their own set of terms, ensuring
clean data to be used inline with the FAIR principles later on.
The :py:class:`~hdmf.term_set.TermSet` class allows for a reusable and sharable
pool of metadata to serve as references for any dataset or attribute.
The :py:class:`~hdmf.term_set.TermSet` class is used closely with
:py:class:`~hdmf.common.resources.HERD` to more efficiently map terms
to data.

In order to actually use a :py:class:`~hdmf.term_set.TermSet`, users will use the
:py:class:`~hdmf.term_set.TermSetWrapper` to wrap data and attributes. The
:py:class:`~hdmf.term_set.TermSetWrapper` uses a user-provided :py:class:`~hdmf.term_set.TermSet`
to perform validation.

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
3. The schema uses LinkML enumerations to list all the possible terms. To define the all the permissible
   values, the user can define them manually in the schema, transfer them from a Google spreadsheet,
   or pull them into the schema dynamically from a LinkML supported source.

For a clear example, please view the
`example_term_set.yaml <https://github.com/hdmf-dev/hdmf/blob/dev/docs/gallery/example_term_set.yaml>`_
for this tutorial, which provides a concise example of how a term set schema looks.

.. note::
    For more information regarding LinkML Enumerations, please refer to
    https://linkml.io/linkml/intro/tutorial06.html.

.. note::
    For more information on how to properly format the Google spreadsheet to be compatible with LinkMl, please
    refer to https://linkml.io/schemasheets/#examples.

.. note::
    For more information how to properly format the schema to support LinkML Dynamic Enumerations, please
    refer to https://linkml.io/linkml/schemas/enums.html#dynamic-enums.
"""
from hdmf.common import DynamicTable, VectorData
import os

try:
    import linkml_runtime  # noqa: F401
except ImportError as e:
    raise ImportError("Please install linkml-runtime to run this example: pip install linkml-runtime") from e
from hdmf.term_set import TermSet, TermSetWrapper

try:
    dir_path = os.path.dirname(os.path.abspath(__file__))
    yaml_file = os.path.join(dir_path, 'example_term_set.yaml')
    schemasheets_folder = os.path.join(dir_path, 'schemasheets')
    dynamic_schema_path = os.path.join(dir_path, 'example_dynamic_term_set.yaml')
except NameError:
    dir_path = os.path.dirname(os.path.abspath('.'))
    yaml_file = os.path.join(dir_path, 'gallery/example_term_set.yaml')
    schemasheets_folder = os.path.join(dir_path, 'gallery/schemasheets')
    dynamic_schema_path = os.path.join(dir_path, 'gallery/example_dynamic_term_set.yaml')

# Use Schemasheets to create TermSet schema
# -----------------------------------------
# The :py:class:`~hdmf.term_set.TermSet` class builds off of LinkML Schemasheets, allowing users to convert between
# a Google spreadsheet to a complete LinkML schema. Once the user has defined the necessary LinkML metadata within the
# spreadsheet, the spreadsheet needs to be saved as individual tsv files, i.e., one tsv file per spreadsheet tab. Please
# refer to the Schemasheets tutorial link above for more details on the required syntax structure within the sheets.
# Once the tsv files are in a folder, the user simply provides the path to the folder with ``schemasheets_folder``.
termset = TermSet(schemasheets_folder=schemasheets_folder)

# Use Dynamic Enumerations to populate TermSet
# --------------------------------------------
# The :py:class:`~hdmf.term_set.TermSet` class allows user to skip manually defining permissible values, by pulling from
# a LinkML supported source. These sources contain multiple ontologies. A user can select a node from an ontology,
# in which all the elements on the branch, starting from the chosen node, will be used as permissible values.
# Please refer to the LinkMl Dynamic Enumeration tutorial for more information on these sources and how to setup Dynamic
# Enumerations within the schema. Once the schema is ready, the user provides a path to the schema and set
# ``dynamic=True``. A new schema, with the populated permissible values, will be created in the same directory.
termset = TermSet(term_schema_path=dynamic_schema_path, dynamic=True)

######################################################
# Viewing TermSet values
# ----------------------------------------------------
# :py:class:`~hdmf.term_set.TermSet` has methods to retrieve terms. The :py:func:`~hdmf.term_set.TermSet.view_set`
# method will return a dictionary of all the terms and the corresponding information for each term.
# Users can index specific terms from the :py:class:`~hdmf.term_set.TermSet`. LinkML runtime will need to be installed.
# You can do so by first running ``pip install linkml-runtime``.
terms = TermSet(term_schema_path=yaml_file)
print(terms.view_set)

# Retrieve a specific term
terms['Homo sapiens']

######################################################
# Validate Data with TermSetWrapper
# ----------------------------------------------------
# :py:class:`~hdmf.term_set.TermSetWrapper` can be wrapped around data.
# To validate data, the user will set the data to the wrapped data, in which validation must pass
# for the data object to be created.
data = VectorData(
    name='species',
    description='...',
    data=TermSetWrapper(value=['Homo sapiens'], termset=terms)
    )

######################################################
# Validate Attributes with TermSetWrapper
# ----------------------------------------------------
# Similar to wrapping datasets, :py:class:`~hdmf.term_set.TermSetWrapper` can be wrapped around any attribute.
# To validate attributes, the user will set the attribute to the wrapped value, in which validation must pass
# for the object to be created.
data = VectorData(
    name='species',
    description=TermSetWrapper(value='Homo sapiens', termset=terms),
    data=['Human']
    )

######################################################
# Validate on append with TermSetWrapper
# ----------------------------------------------------
# As mentioned prior, when using a :py:class:`~hdmf.term_set.TermSetWrapper`, all new data is validated.
# This is true for adding new data with append and extend.
data = VectorData(
    name='species',
    description='...',
    data=TermSetWrapper(value=['Homo sapiens'], termset=terms)
    )

data.append('Ursus arctos horribilis')
data.extend(['Mus musculus', 'Myrmecophaga tridactyla'])

######################################################
# Validate Data in a DynamicTable
# ----------------------------------------------------
# Validating data for :py:class:`~hdmf.common.table.DynamicTable` is determined by which columns were
# initialized with a :py:class:`~hdmf.term_set.TermSetWrapper`. The data is validated when the columns
# are created and modified using ``DynamicTable.add_row``.
col1 = VectorData(
    name='Species_1',
    description='...',
    data=TermSetWrapper(value=['Homo sapiens'], termset=terms),
)
col2 = VectorData(
    name='Species_2',
    description='...',
    data=TermSetWrapper(value=['Mus musculus'], termset=terms),
)
species = DynamicTable(name='species', description='My species', columns=[col1,col2])

##########################################################
# Validate new rows in a DynamicTable with TermSetWrapper
# --------------------------------------------------------
# Validating new rows to :py:class:`~hdmf.common.table.DynamicTable` is simple. The
# :py:func:`~hdmf.common.table.DynamicTable.add_row` method will automatically check each column for a
# :py:class:`~hdmf.term_set.TermSetWrapper`. If a wrapper is being used, then the data will be
# validated for that column using that column's :py:class:`~hdmf.term_set.TermSet` from the
# :py:class:`~hdmf.term_set.TermSetWrapper`. If there is invalid data, the
# row will not be added and the user will be prompted to fix the new data in order to populate the table.
species.add_row(Species_1='Mus musculus', Species_2='Mus musculus')

#############################################################
# Validate new columns in a DynamicTable with TermSetWrapper
# -----------------------------------------------------------
# To add a column that is validated using :py:class:`~hdmf.term_set.TermSetWrapper`,
# wrap the data in the :py:func:`~hdmf.common.table.DynamicTable.add_column`
# method as if you were making a new instance of :py:class:`~hdmf.common.table.VectorData`.
species.add_column(name='Species_3',
                   description='...',
                   data=TermSetWrapper(value=['Ursus arctos horribilis', 'Mus musculus'], termset=terms),)
