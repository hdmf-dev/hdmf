"""
DynamicTable
============

This is a user guide to working with the :py:class:`~hdmf.common.table.DynamicTable` class.

"""

###############################################################################
# Introduction
# ------------
# The :py:class:`~hdmf.common.table.DynamicTable` class represents a column-based table
# to which you can add custom columns. It consists of a name, a description, a list of
# row IDs, and a list of columns. Columns are represented by
# :py:class:`~hdmf.common.table.VectorData` and :py:class:`~hdmf.common.table.VectorIndex`
# objects.

###############################################################################
# Constructing a table
# --------------------
# To create a :py:class:`~hdmf.common.table.DynamicTable`, call the constructor for
# :py:class:`~hdmf.common.table.DynamicTable` with a string ``name`` and string
# ``description``. Specifying the arguments with keywords is recommended.

from hdmf.common import DynamicTable

table = DynamicTable(
    name='my table',
    description='an example table',
)

###############################################################################
# Initializing columns
# --------------------
# You can initialize a :py:class:`~hdmf.common.table.DynamicTable` with particular
# columns by passing a list or tuple of
# :py:class:`~hdmf.common.table.VectorData` objects for the ``columns`` argument
# in the constructor.
#
# If the :py:class:`~hdmf.common.table.VectorData` objects contain data values,
# then each :py:class:`~hdmf.common.table.VectorData` object must contain the
# same number of rows as each other. A list of row IDs may be passed into the
# :py:class:`~hdmf.common.table.DynamicTable` constructor using the ``id``
# argument. If IDs are passed in, there should be the same number of rows as
# the column data. If IDs are not passed in, then the IDs will be set to
# ``range(0, len(column_data))`` by default.

from hdmf.common import VectorData, VectorIndex

col1 = VectorData(
    name='col1',
    description='column #1',
    data=[1, 2],
)
col2 = VectorData(
    name='col2',
    description='column #2',
    data=['a', 'b'],
)

# this table will have two rows with ids 0 and 1
table = DynamicTable(
    name='my table',
    description='an example table',
    columns=[col1, col2],
)

# this table will have two rows with ids 100 and 200
table_set_ids = DynamicTable(
    name='my table',
    description='an example table',
    columns=[col1, col2],
    id=[100, 200],
)

###############################################################################
# Adding rows
# -----------
# You can add rows to a :py:class:`~hdmf.common.table.DynamicTable` using
# :py:meth:`DynamicTable.add_row <hdmf.common.table.DynamicTable.add_row>`.
# A keyword argument for every column in the table must be supplied.

table.add_row(
    col1=3,
    col2='c',
)

###############################################################################
# You can also supply an optional row ID to
# :py:meth:`DynamicTable.add_row <hdmf.common.table.DynamicTable.add_row>`.
# If no ID is supplied, the ID is automatically set to the number of rows in the table
# prior to adding the new row (i.e., automatic IDs start at 0).

table.add_row(
    col1=4,
    col2='d',
    id=10,
)

###############################################################################
# .. note::
#
#   Row IDs are not required to be unique. However, if ``enforce_unique_id=True``
#   is passed, then adding a row with an ID that already exists in the table will
#   raise an error.

###############################################################################
# Adding columns
# --------------
# You can add columns to a :py:class:`~hdmf.common.table.DynamicTable` using
# :py:meth:`DynamicTable.add_column <hdmf.common.table.DynamicTable.add_column>`.
# If the table already has rows, then the ``data`` argument must be supplied
# as a list of values, one for each row in the table.

table.add_column(
    name='col3',
    description='column #3',
    data=[True, True, False, True],  # specify data for the 4 rows in the table
)

###############################################################################
# Ragged array columns
# ^^^^^^^^^^^^^^^^^^^^
# A table column with a different number of elements for each row is called a
# ragged array. To initialize a :py:class:`~hdmf.common.table.DynamicTable`
# with a ragged array column, pass both
# the :py:class:`~hdmf.common.table.VectorIndex` and its target
# :py:class:`~hdmf.common.table.VectorData` object in for the ``columns``
# argument in the constructor.

col1 = VectorData(
    name='col1',
    description='column #1',
    data=['1a', '1b', '1c', '2a'],
)
col1_ind = VectorIndex(
    name='col1_index',
    target=col1,
    data=[3, 4],
)

table_ragged_col = DynamicTable(
    name='my table',
    description='an example table',
    columns=[col1, col1_ind],
)

###############################################################################
# You can add a ragged array column to an existing
# :py:class:`~hdmf.common.table.DynamicTable` by specifying ``index=True``
# to :py:meth:`DynamicTable.add_column <hdmf.common.table.DynamicTable.add_column>`.

new_table = DynamicTable(
    name='my table',
    description='an example table',
)

new_table.add_column(
    name='col4',
    description='column #4',
    index=True,
)

###############################################################################
# If the table already contains data, you must specify the new column values for
# the existing rows using the ``data`` argument and you must specify the end indices of
# the ``data`` argument that correspond to each row as a list/tuple/array of values for
# the ``index`` argument.

table.add_column(
    name='col4',
    description='column #4',
    data=[1, 0, -1, 0, -1, 1, 1, -1],
    index=[3, 4, 6, 8],  # specify the end indices of data for each row
)

###############################################################################
# Converting the table to a pandas ``DataFrame``
# ----------------------------------------------
# `pandas`_ is a popular data analysis tool, especially for working with tabular data.
# You can convert your :py:class:`~hdmf.common.table.DynamicTable` to a
# :py:class:`~pandas.DataFrame` using
# :py:meth:`DynamicTable.to_dataframe <hdmf.common.table.DynamicTable.to_dataframe>`.
# Accessing the table as a :py:class:`~pandas.DataFrame` provides you with powerful,
# standard methods for indexing, selecting, and querying tabular data from `pandas`_,
# and is recommended. See also the `pandas indexing documentation`_.
# Printing a :py:class:`~hdmf.common.table.DynamicTable` as a :py:class:`~pandas.DataFrame`
# or displaying the :py:class:`~pandas.DataFrame` in Jupyter shows a more intuitive
# tabular representation of the data than printing the
# :py:class:`~hdmf.common.table.DynamicTable` object.
#
# .. _pandas: https://pandas.pydata.org/
# .. _`pandas indexing documentation`: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html

df = table.to_dataframe()

###############################################################################
# .. note::
#
#   Changes to the ``DataFrame`` will not be saved in the ``DynamicTable``.

###############################################################################
# Converting the table from a pandas ``DataFrame``
# ------------------------------------------------
# If your data is already in a :py:class:`~pandas.DataFrame`, you can convert the
# ``DataFrame`` to a :py:class:`~hdmf.common.table.DynamicTable` using the class method
# :py:meth:`DynamicTable.from_dataframe <hdmf.common.table.DynamicTable.from_dataframe>`.

table_from_df = DynamicTable.from_dataframe(
    name='my table',
    df=df,
)

###############################################################################
# Accessing elements
# ------------------
# To access an element in the i-th row in the column with name "col_name" in a
# :py:class:`~hdmf.common.table.DynamicTable`, use square brackets notation:
# ``table[i, col_name]``. You can also use a tuple of row index and column
# name within the square brackets.

table[0, 'col1']  # returns 1
table[(0, 'col1')]  # returns 1

###############################################################################
# If the column is a ragged array, instead of a single value being returned,
# a list of values for that element is returned.

table[0, 'col4']  # returns [1, 0, -1]

###############################################################################
# Standard Python and numpy slicing can be used for the row index.

import numpy as np

table[:2, 'col1']  # get a list of elements from the first two rows at column 'col1'
table[0:10:2, 'col1']  # get a list of elements from rows 0 to 10 (exclusive) in steps of 2 at column 'col1'
table[10::-1, 'col1']  # get a list of elements from rows 10 to 0 in reverse order at column 'col1'
table[slice(0, 10, 2), 'col1']  # equivalent to table[0:4:2, 'col1']
table[np.s_[0:10:2], 'col1']  # equivalent to table[0:10:2, 'col1']

###############################################################################
# If the column is a ragged array, instead of a list of row values being returned,
# a list of list elements for the selected rows is returned.

table[:2, 'col4']  # returns [[1, 0, -1], [0]]

###############################################################################
# .. note::
#
#   You cannot supply a list/tuple for the row index or column name. For this
#   kind of access, first convert the :py:class:`~hdmf.common.table.DynamicTable`
#   to a :py:class:`~pandas.DataFrame`.

###############################################################################
# Accessing columns
# -----------------
# To access all the values in a column, use square brackets with a colon for the
# row index: ``table[:, col_name]``. If the column is a ragged array, a list of
# list elements is returned.

table[:, 'col1']  # returns [1, 2, 3, 4]
table[:, 'col4']  # returns [[1, 0, -1], [0], [-1, 1], [1, -1]]

###############################################################################
# Accessing rows
# --------------
# To access the i-th row in a :py:class:`~hdmf.common.table.DynamicTable`, returned
# as a :py:class:`~pandas.DataFrame`, use the syntax ``table[i]``. Standard Python
# and numpy slicing can be used for the row index.

table[0]  # get the 0th row of the table as a DataFrame
table[:2]  # get the first two rows
table[0:10:2]  # get rows 0 to 10 (exclusive) in steps of 2
table[10::-1]  # get rows 10 to 0 in reverse order
table[slice(0, 10, 2)]  # equivalent to table[0:10:2, 'col1']
table[np.s_[0:10:2]]  # equivalent to table[0:10:2, 'col1']

###############################################################################
# .. note::
#
#   The syntax ``table[i]`` returns the i-th row, NOT the row with ID of `i`.

###############################################################################
# .. note::
#
#   Do not access a set of rows by supplying a list/tuple of row indices. This
#   syntax will instead return the table element at the row index corresponding to
#   the first element of the list/tuple and the column index corresponding to
#   the second element.

###############################################################################
# Accessing the column data types
# -------------------------------
# To access the :py:class:`~hdmf.common.table.VectorData` or
# :py:class:`~hdmf.common.table.VectorIndex` object representing a column, you
# can use three different methods. Use the column name in square brackets, e.g.,
# ``table.[col_name]``, use the
# :py:meth:`DynamicTable.get <hdmf.common.table.DynamicTable.get>` method, or
# use the column name as an attribute, e.g., ``table.col_name``.

table['col1']
table.get('col1')  # equivalent to table['col1'] except this returns None if 'col1' is not found
table.get('col1', default=0)  # you can change the default return value
table.col1

###############################################################################
# .. note::
#
#   Using the column name as an attribute does NOT work if the column name is
#   the same as a non-column name attribute or method of the
#   :py:class:`~hdmf.common.table.DynamicTable` class,
#   e.g., ``name``, ``description``, ``object_id``, ``parent``, ``modified``.

###############################################################################
# If the column is a ragged array, then the methods above will return the
# :py:class:`~hdmf.common.table.VectorIndex` associated with the ragged array.

table['col4']
table.get('col4')  # equivalent to table['col4'] except this returns None if 'col4' is not found
table.get('col4', default=0)  # you can change the default return value

###############################################################################
# .. note::
#
#   The attribute syntax ``table.col_name`` currently returns the ``VectorData``
#   instead of the ``VectorIndex`` for a ragged array. This is a known
#   issue and will be fixed in a future version of HDMF.

###############################################################################
# Accessing elements from column data types
# -----------------------------------------
# Standard Python and numpy slicing can be used on the
# :py:class:`~hdmf.common.table.VectorData` or
# :py:class:`~hdmf.common.table.VectorIndex` objects to access elements from
# column data. If the column is a ragged array, then instead of a list of row
# values being returned, a list of list elements for the selected rows is returned.

table['col1'][0]  # get the 0th element from column 'col1'
table['col1'][:2]  # get a list of the 0th and 1st elements
table['col1'][0:10:2]  # get a list of the 0th to 10th (exclusive) elements in steps of 2
table['col1'][10::-1]  # get a list of the 10th to 0th elements in reverse order
table['col1'][slice(0, 10, 2)]  # equivalent to table['col1'][0:10:2]
table['col1'][np.s_[0:10:2]]  # equivalent to table['col1'][0:10:2]

table['col4'][:2]  # get a list of the 0th and 1st list elements

###############################################################################
# .. note::
#
#   The syntax ``table[col_name][i]`` is equivalent to ``table[i, col_name]``.

###############################################################################
# .. note::
#
#   It is also possible to access columns by column index using ``table[:, j]``
#   and elements by row index and column index using ``table[i, j]``. These are
#   equivalent to ``table.columns[j][:]`` and ``table.columns[j][i]`` and are
#   not recommended because they interact with the internal list of columns.

###############################################################################
# Referencing rows of a DynamicTable
# ----------------------------------
# TODO

###############################################################################
# Creating custom DynamicTable subclasses
# ---------------------------------------
# TODO

###############################################################################
# Defining ``__columns__``
# ^^^^^^^^^^^^^^^^^^^^^^^^
# TODO
