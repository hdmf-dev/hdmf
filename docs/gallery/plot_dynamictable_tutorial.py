"""

.. _dynamictable-tutorial:

DynamicTable Tutorial
=====================

This is a tutorial for interacting with :py:class:`~hdmf.common.table.DynamicTable` objects. This tutorial
is written for beginners and does not describe the full capabilities and nuances
of :py:class:`~hdmf.common.table.DynamicTable` functionality. Please see the :ref:`dynamictable-howtoguide`
for more complete documentation. This tutorial is designed to give
you basic familiarity with how :py:class:`~hdmf.common.table.DynamicTable` works and help you get started
with creating a :py:class:`~hdmf.common.table.DynamicTable`, adding columns and rows to a
:py:class:`~hdmf.common.table.DynamicTable`,
and accessing data in a :py:class:`~hdmf.common.table.DynamicTable`.

Introduction
------------
The :py:class:`~hdmf.common.table.DynamicTable` class represents a column-based table
to which you can add custom columns. It consists of a name, a description, a list of
row IDs, and a list of columns.

Constructing a table
--------------------
To create a :py:class:`~hdmf.common.table.DynamicTable`, call the constructor for
:py:class:`~hdmf.common.table.DynamicTable` with a string ``name`` and string
``description``.

"""

# sphinx_gallery_thumbnail_path = 'figures/gallery_thumbnail_dynamictable.png'
from hdmf.common import DynamicTable

users_table = DynamicTable(
    name='users',
    description='a table containing data/metadata about users, one user per row',
)

###############################################################################
# Adding columns
# --------------
# You can add columns to a :py:class:`~hdmf.common.table.DynamicTable` using
# :py:meth:`DynamicTable.add_column <hdmf.common.table.DynamicTable.add_column>`.

users_table.add_column(
    name='first_name',
    description='the first name of the user',
)

users_table.add_column(
    name='last_name',
    description='the last name of the user',
)

###############################################################################
# Adding ragged array columns
# ---------------------------
# You may want to add columns to your table that have a different number of entries per row.
# This is called a "ragged array column". To do this, pass ``index=True`` to
# :py:meth:`DynamicTable.add_column <hdmf.common.table.DynamicTable.add_column>`.

users_table.add_column(
    name='phone_number',
    description='the phone number of the user',
    index=True,
)

###############################################################################
# Adding rows
# -----------
# You can add rows to a :py:class:`~hdmf.common.table.DynamicTable` using
# :py:meth:`DynamicTable.add_row <hdmf.common.table.DynamicTable.add_row>`.
# You must pass in a keyword argument for every column in the table.
# Ragged array column arguments should be passed in as lists or numpy arrays.
# The ID of the row will automatically be set and incremented for every row,
# starting at 0.

# id will be set to 0 automatically
users_table.add_row(
    first_name='Grace',
    last_name='Hopper',
    phone_number=['123-456-7890'],
)

# id will be set to 1 automatically
users_table.add_row(
    first_name='Alan',
    last_name='Turing',
    phone_number=['555-666-7777', '888-111-2222'],
)

###############################################################################
# Displaying the table contents as a pandas ``DataFrame``
# -------------------------------------------------------
# `pandas`_ is a popular data analysis tool for working with tabular data.
# Convert your :py:class:`~hdmf.common.table.DynamicTable` to a pandas
# :py:class:`~pandas.DataFrame` using
# :py:meth:`DynamicTable.to_dataframe <hdmf.common.table.DynamicTable.to_dataframe>`.
#
# .. _pandas: https://pandas.pydata.org/

users_df = users_table.to_dataframe()
users_df

###############################################################################
# Accessing the table as a :py:class:`~pandas.DataFrame` provides you with powerful
# methods for indexing, selecting, and querying tabular data from `pandas`_.

###############################################################################
# Get the "last_name" column as a pandas :py:class:`~pandas.Series`:
users_df['last_name']

###############################################################################
# The index of the :py:class:`~pandas.DataFrame` is automatically set to the
# table IDs. Get the row with ID = 0 as a pandas :py:class:`~pandas.Series`:
users_df.loc[0]

###############################################################################
# Get single cells of the table by indexing with both ID and column name:
print('My first user:', users_df.loc[0, 'first_name'], users_df.loc[0, 'last_name'])

###############################################################################
# Adding columns that reference rows of other ``DynamicTable`` objects
# --------------------------------------------------------------------
# You can create a column that references rows of another
# :py:class:`~hdmf.common.table.DynamicTable`. This is analogous to
# a foreign key in a relational database. To do this, use the ``table`` keyword
# argument for
# :py:meth:`DynamicTable.add_column <hdmf.common.table.DynamicTable.add_column>`
# and set it to the other table.

# create a new table of users
users_table = DynamicTable(
    name='users',
    description='a table containing data/metadata about users, one user per row',
)

# add simple columns to this table
users_table.add_column(
    name='first_name',
    description='the first name of the user',
)
users_table.add_column(
    name='last_name',
    description='the last name of the user',
)

# create a new table of addresses to reference
addresses_table = DynamicTable(
    name='addresses',
    description='a table containing data/metadata about addresses, one address per row',
)
addresses_table.add_column(
    name='street_address',
    description='the street number and address',
)
addresses_table.add_column(
    name='city',
    description='the city of the address',
)

# add rows to the addresses table
addresses_table.add_row(
    street_address='123 Main St',
    city='Springfield'
)
addresses_table.add_row(
    street_address='45 British Way',
    city='London'
)

# add a column to the users table that references rows of the addresses table
users_table.add_column(
    name='address',
    description='the address of the user',
    table=addresses_table
)

# add rows to the users table
users_table.add_row(
    first_name='Grace',
    last_name='Hopper',
    address=0  # <-- row index of the address table
)

users_table.add_row(
    first_name='Alan',
    last_name='Turing',
    address=1  # <-- row index of the address table
)

###############################################################################
# Displaying the contents of a table with references to another table
# -------------------------------------------------------------------
# Earlier, we converted a :py:class:`~hdmf.common.table.DynamicTable` to a
# :py:class:`~pandas.DataFrame` using
# :py:meth:`DynamicTable.to_dataframe <hdmf.common.table.DynamicTable.to_dataframe>`
# and printed the :py:class:`~pandas.DataFrame` to see its contents.
# This also works when the :py:class:`~hdmf.common.table.DynamicTable` contains a column
# that references another table. However, the entries for this column for each row
# will be printed as a nested :py:class:`~pandas.DataFrame`. This can be difficult to
# read, so to view only the row indices of the referenced table, pass
# ``index=True`` to
# :py:meth:`DynamicTable.to_dataframe <hdmf.common.table.DynamicTable.to_dataframe>`.
users_df = users_table.to_dataframe(index=True)
users_df

###############################################################################
# You can then access the referenced table using the ``table`` attribute of the
# column object. This is useful when reading a table from a file where you may not have
# a variable to access the referenced table.
#
# First, use :py:meth:`DynamicTable.__getitem__ <hdmf.common.table.DynamicTable.__getitem__>`
# (square brackets notation) to get the
# :py:class:`~hdmf.common.table.DynamicTableRegion` object representing the column.
# Then access its ``table`` attribute to get the addresses table and convert the table
# to a :py:class:`~pandas.DataFrame`.
address_column = users_table['address']
read_addresses_table = address_column.table
addresses_df = read_addresses_table.to_dataframe()

###############################################################################
# Get the addresses corresponding to the rows of the users table:
address_indices = users_df['address']  # pandas Series of row indices into the addresses table
addresses_df.iloc[address_indices]  # use .iloc because these are row indices not ID values

###############################################################################
# .. note::
#   The indices returned by ``users_df['address']`` are row indices and not
#   the ID values of the table. However, if you are using default IDs, these
#   values will be the same.

###############################################################################
# You now know the basics of creating :py:class:`~hdmf.common.table.DynamicTable`
# objects and reading data from them, including tables that have ragged array columns
# and references to other tables. Learn more about working with
# :py:class:`~hdmf.common.table.DynamicTable` in the :ref:`dynamictable-howtoguide`,
# including:
#
# * ragged array columns with references to other tables
# * nested ragged array columns
# * columns with multidimensional array data
# * columns with enumerated (categorical) data
# * accessing data and properties from the column objects directly
# * writing and reading tables to a file
# * writing expandable tables
# * defining subclasses of :py:class:`~hdmf.common.table.DynamicTable`
