"""

.. _dynamictable-howtoguide:

DynamicTable How-To Guide
=========================

This is a user guide to interacting with ``DynamicTable`` objects.

"""

###############################################################################
# Introduction
# ------------
# The :py:class:`~hdmf.common.table.DynamicTable` class represents a column-based table
# to which you can add custom columns. It consists of a name, a description, a list of
# row IDs, and a list of columns. Columns are represented by objects of the class
# :py:class:`~hdmf.common.table.VectorData`, including subclasses of
# :py:class:`~hdmf.common.table.VectorData`, such as :py:class:`~hdmf.common.table.VectorIndex`,
# and :py:class:`~hdmf.common.table.DynamicTableRegion`.

###############################################################################
# Constructing a table
# --------------------
# To create a :py:class:`~hdmf.common.table.DynamicTable`, call the constructor for
# :py:class:`~hdmf.common.table.DynamicTable` with a string ``name`` and string
# ``description``. Specifying the arguments with keywords is recommended.

# sphinx_gallery_thumbnail_path = 'figures/gallery_thumbnail_dynamictable.png'
from hdmf.common import DynamicTable

table = DynamicTable(
    name='my_table',
    description='an example table',
)

###############################################################################
# Initializing columns
# --------------------
# You can create a :py:class:`~hdmf.common.table.DynamicTable` with particular
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
# ``range(len(column_data))`` by default.

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

# this table will have two rows with ids 0 and 1
table_set_ids = DynamicTable(
    name='my table',
    description='an example table',
    columns=[col1, col2],
    id=[0, 1],
)

###############################################################################
# If a list of integers in passed to ``id``,
# :py:class:`~hdmf.common.table.DynamicTable` automatically creates
# an :py:class:`~hdmf.common.table.ElementIdentifiers` object, which is the data type
# that stores row IDs. The above command is equivalent to:

from hdmf.common.table import ElementIdentifiers

table_set_ids = DynamicTable(
    name='my table',
    description='an example table',
    columns=[col1, col2],
    id=ElementIdentifiers(name='id', data=[0, 1]),
)

###############################################################################
# Adding rows
# -----------
# You can also add rows to a :py:class:`~hdmf.common.table.DynamicTable` using
# :py:meth:`DynamicTable.add_row <hdmf.common.table.DynamicTable.add_row>`.
# A keyword argument for every column in the table must be supplied.
# You may also supply an optional row ID.

table.add_row(
    col1=3,
    col2='c',
    id=2,
)

###############################################################################
# .. note::
#   If no ID is supplied, the row ID is automatically set to the number of rows of the table prior to adding the new
#   row. This can result in duplicate IDs. In general, IDs should be unique, but this is not enforced by default.
#   Pass `enforce_unique_id=True` to :py:meth:`DynamicTable.add_row <hdmf.common.table.DynamicTable.add_row>`
#   to raise an error if the ID is set to an existing ID value.

# this row will have ID 3 by default
table.add_row(
    col1=4,
    col2='d',
)

###############################################################################
# Adding columns
# --------------
# You can add columns to a :py:class:`~hdmf.common.table.DynamicTable` using
# :py:meth:`DynamicTable.add_column <hdmf.common.table.DynamicTable.add_column>`.
# If the table already has rows, then the ``data`` argument must be supplied
# as a list of values, one for each row already in the table.

table.add_column(
    name='col3',
    description='column #3',
    data=[True, True, False, True],  # specify data for the 4 rows in the table
)

###############################################################################
# Enumerated (categorical) data
# -----------------------------
# :py:class:`~hdmf.common.table.EnumData` is a special type of column for storing
# an enumerated data type. This way each unique value is stored once, and the data
# references those values by index. Using this method is more efficient than storing
# a single value many times, and has the advantage of communicating to downstream
# tools that the data is categorical in nature.
#
# .. warning::
#
#    :py:class:`~hdmf.common.table.EnumData` is currently an experimental
#    feature and as such should not be used for production use.
#

from hdmf.common.table import EnumData
import warnings
warnings.filterwarnings(action="ignore", message="EnumData is experimental")

# this column has a length of 5, not 3. the first row has value "aa"
enum_col = EnumData(
    name='cell_type',
    description='this column holds categorical variables',
    data=[0, 1, 2, 1, 0],
    elements=['aa', 'bb', 'cc']
)

my_table = DynamicTable(
    name='my_table',
    description='an example table',
    columns=[enum_col],
)

###############################################################################
# Ragged array columns
# --------------------
# A table column with a different number of elements for each row is called a
# "ragged array column". To initialize a :py:class:`~hdmf.common.table.DynamicTable`
# with a ragged array column, pass both
# the :py:class:`~hdmf.common.table.VectorIndex` and its target
# :py:class:`~hdmf.common.table.VectorData` in for the ``columns``
# argument in the constructor. For instance, the following code creates a column
# called ``col1`` where the first cell is ['1a', '1b', '1c'] and the second cell
# is ['2a'].

col1 = VectorData(
    name='col1',
    description='column #1',
    data=['1a', '1b', '1c', '2a'],
)
# the 3 signifies that elements 0 to 3 (exclusive) of the target column belong to the first row
# the 4 signifies that elements 3 to 4 (exclusive) of the target column belong to the second row
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

####################################################################################
# .. note::
#   By convention, the name of the :py:class:`~hdmf.common.table.VectorIndex` should be
#   the name of the target column with the added suffix "_index".

####################################################################################
# VectorIndex.data provides the indices for how to break VectorData.data into cells
#
# You can add an empty ragged array column to an existing
# :py:class:`~hdmf.common.table.DynamicTable` by specifying ``index=True``
# to :py:meth:`DynamicTable.add_column <hdmf.common.table.DynamicTable.add_column>`.
# This method only works if run before any rows have been added to the table.

new_table = DynamicTable(
    name='my_table',
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

table.add_column(  # <-- this table already has 4 rows
    name='col4',
    description='column #4',
    data=[1, 0, -1, 0, -1, 1, 1, -1],
    index=[3, 4, 6, 8],  # specify the end indices (exclusive) of data for each row
)

###############################################################################
# Alternatively we may also define the ragged array data as a nested list
# and use the ``index`` argument to indicate the number of levels. In this case,
# the :py:class:`~hdmf.common.table.DynamicTable.add_column` function will
# automatically flatten the data array and compute the corresponding index vectors.

table.add_column(  # <-- this table already has 4 rows
    name='col5',
    description='column #5',
    data=[[[1, ], [2, 2]],       # row 1
          [[3, 3], ],            # row 2
          [[4, ], [5, 5]],       # row 3
          [[6, 6], [7, 7, 7]]],  # row 4
    index=2   # number of levels in the ragged array
)
# Show that the ragged array was converted to flat VectorData with a double VectorIndex
print("Flattened data: %s" % str(table.col5.data))
print("Level 1 index: %s" % str(table.col5_index.data))
print("Level 2 index: %s" % str(table.col5_index_index.data))


###############################################################################
# Referencing rows of other tables
# --------------------------------
# You can create a column that references rows of another table by adding a
# :py:class:`~hdmf.common.table.DynamicTableRegion` object as a column of your
# :py:class:`~hdmf.common.table.DynamicTable`. This is analogous to
# a foreign key in a relational database.

from hdmf.common.table import DynamicTableRegion

dtr_col = DynamicTableRegion(
    name='table1_ref',
    description='references rows of earlier table',
    data=[0, 1, 0, 0],  # refers to row indices of the 'table' variable
    table=table
)

data_col = VectorData(
    name='col2',
    description='column #2',
    data=['a', 'a', 'a', 'b'],
)

table2 = DynamicTable(
    name='my_table',
    description='an example table',
    columns=[dtr_col, data_col],
)

###############################################################################
# Here, the ``data`` of ``dtr_col`` maps to rows of ``table`` (0-indexed).
#
# .. note::
#   The ``data`` values of :py:class:`~hdmf.common.table.DynamicTableRegion` map to the row
#   index, not the row ID, though if you are using default IDs, these values will be the
#   same.
#
# Reference more than one row of another table with a
# :py:class:`~hdmf.common.table.DynamicTableRegion` indexed by a
# :py:class:`~hdmf.common.table.VectorIndex`.

indexed_dtr_col = DynamicTableRegion(
    name='table1_ref2',
    description='references multiple rows of earlier table',
    data=[0, 0, 1, 1, 0, 0, 1],
    table=table
)

# row 0 refers to rows [0, 0], row 1 refers to rows [1], row 2 refers to rows [1, 0], row 3 refers to rows [0, 1] of
# the "table" variable
dtr_idx = VectorIndex(
    name='table1_ref2_index',
    target=indexed_dtr_col,
    data=[2, 3, 5, 7],
)

table3 = DynamicTable(
    name='my_table',
    description='an example table',
    columns=[dtr_idx, indexed_dtr_col],
)

###############################################################################
# Setting the target table of a DynamicTableRegion column of a DynamicTable
# -------------------------------------------------------------------------
# A subclass of DynamicTable might have a pre-defined DynamicTableRegion column.
# To write this column correctly, the "table" attribute of the column must be set so
# that users know to what table the row index values reference. Because the target
# table could be any table, the "table" attribute must be set explicitly. There are three
# ways to do so. First, you can use the ``target_tables`` argument of the
# DynamicTable constructor as shown below. This argument
# is a dictionary mapping the name of the DynamicTableRegion column to
# the target table. Secondly, the target table can be set after the DynamicTable
# has been initialized using ``my_table.my_column.table = other_table``. Finally,
# you can create the DynamicTableRegion column and pass the ``table``
# attribute to `DynamicTableRegion.__init__` and then pass the column to
# `DynamicTable.__init__` using the `columns` argument. However, this approach
# is not recommended for columns defined in the schema, because it is up to
# the user to ensure that the column is created in accordance with the schema.

class SubTable(DynamicTable):
    __columns__ = (
        {'name': 'dtr', 'description': 'required region', 'required': True, 'table': True},
    )

referenced_table = DynamicTable(
    name='referenced_table',
    description='an example table',
)

sub_table = SubTable(
    name='sub_table',
    description='an example table',
    target_tables={'dtr': referenced_table},
)
# now the target table of the DynamicTableRegion column 'dtr' is set to `referenced_table`

###############################################################################
# Creating an expandable table
# ----------------------------
# When using the default HDF5 backend, each column of these tables is an HDF5 Dataset,
# which by default are set in size. This means that once a file is written, it is not
# possible to add a new row. If you want to be able to save this file, load it, and add
# more rows to the table, you will need to set this up when you create the
# :py:class:`~hdmf.common.table.DynamicTable`. You do this by wrapping the data with
# :py:class:`~hdmf.backends.hdf5.h5_utils.H5DataIO` and the argument ``maxshape=(None, )``.

from hdmf.backends.hdf5.h5_utils import H5DataIO

col1 = VectorData(
    name='expandable_col1',
    description='column #1',
    data=H5DataIO(data=[1, 2], maxshape=(None,)),
)
col2 = VectorData(
    name='expandable_col2',
    description='column #2',
    data=H5DataIO(data=['a', 'b'], maxshape=(None,)),
)

# don't forget to wrap the row IDs too!
ids = ElementIdentifiers(
    name='id',
    data=H5DataIO(data=[0, 1], maxshape=(None,)),
)

expandable_table = DynamicTable(
    name='expandable_table',
    description='an example table that can be expanded after being saved to a file',
    columns=[col1, col2],
    id=ids,
)

###############################################################################
# Now you can write the file, read it back, and run ``expandable_table.add_row()``.
# In this example, we are setting ``maxshape`` to ``(None,)``, which means this is a
# 1-dimensional matrix that can expand indefinitely along its single dimension. You
# could also use an integer in place of ``None``. For instance, ``maxshape=(8,)`` would
# allow the column to grow up to a length of 8. Whichever ``maxshape`` you choose,
# it should be the same for all :py:class:`~hdmf.common.table.VectorData` and
# :py:class:`~hdmf.common.table.ElementIdentifiers` objects in the
# :py:class:`~hdmf.common.table.DynamicTable`, since they must always be the same
# length. The default :py:class:`~hdmf.common.table.ElementIdentifiers` automatically
# generated when you pass a list of integers to the ``id`` argument of the
# :py:class:`~hdmf.common.table.DynamicTable` constructor is not expandable, so do not
# forget to create a :py:class:`~hdmf.common.table.ElementIdentifiers` object, and wrap
# that data as well. If any of the columns are indexed, the ``data`` argument of
# :py:class:`~hdmf.common.table.VectorIndex` will also need to be wrapped with
# :py:class:`~hdmf.backends.hdf5.h5_utils.H5DataIO`.
#
#
# Converting the table to a pandas ``DataFrame``
# ----------------------------------------------
# `pandas`_ is a popular data analysis tool, especially for working with tabular data.
# You can convert your :py:class:`~hdmf.common.table.DynamicTable` to a
# :py:class:`~pandas.DataFrame` using
# :py:meth:`DynamicTable.to_dataframe <hdmf.common.table.DynamicTable.to_dataframe>`.
# Accessing the table as a :py:class:`~pandas.DataFrame` provides you with powerful,
# standard methods for indexing, selecting, and querying tabular data from `pandas`_.
# This is the recommended method of reading data from your table. See also the `pandas indexing documentation`_.
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
    name='my_table',
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
table[0:3:2, 'col1']  # get a list of elements from rows 0 to 3 (exclusive) in steps of 2 at column 'col1'
table[3::-1, 'col1']  # get a list of elements from rows 3 to 0 in reverse order at column 'col1'

# the following are equivalent to table[0:3:2, 'col1']
table[slice(0, 3, 2), 'col1']
table[np.s_[0:3:2], 'col1']
table[[0, 2], 'col1']
table[np.array([0, 2]), 'col1']

###############################################################################
# If the column is a ragged array, instead of a list of row values being returned,
# a list of list elements for the selected rows is returned.

table[:2, 'col4']  # returns [[1, 0, -1], [0]]

###############################################################################
# .. note::
#
#   You cannot supply a list/tuple for the column name. For this
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
table[0:3:2]  # get rows 0 to 3 (exclusive) in steps of 2
table[3::-1]  # get rows 3 to 0 in reverse order

# the following are equivalent to table[0:3:2]
table[slice(0, 3, 2)]
table[np.s_[0:3:2]]
table[[0, 2]]
table[np.array([0, 2])]

###############################################################################
# .. note::
#
#   The syntax ``table[i]`` returns the i-th row, NOT the row with ID of `i`.

###############################################################################
# Iterating over rows
# --------------------
# To iterate over the rows of a :py:class:`~hdmf.common.table.DynamicTable`,
# first convert the :py:class:`~hdmf.common.table.DynamicTable` to a
# :py:class:`~pandas.DataFrame` using
# :py:meth:`DynamicTable.to_dataframe <hdmf.common.table.DynamicTable>`.
# For more information on iterating over a :py:class:`~pandas.DataFrame`,
# see https://pandas.pydata.org/pandas-docs/stable/user_guide/basics.html#iteration

df = table.to_dataframe()
for row in df.itertuples():
    print(row)

###############################################################################
# Accessing the column data types
# -------------------------------
# To access the :py:class:`~hdmf.common.table.VectorData` or
# :py:class:`~hdmf.common.table.VectorIndex` object representing a column, you
# can use three different methods. Use the column name in square brackets, e.g.,
# ``table[col_name]``, use the
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
table['col1'][0:3:2]  # get a list of the 0th to 3rd (exclusive) elements in steps of 2
table['col1'][3::-1]  # get a list of the 3rd to 0th elements in reverse order

# the following are equivalent to table['col1'][0:3:2]
table['col1'][slice(0, 3, 2)]
table['col1'][np.s_[0:3:2]]
table['col1'][[0, 2]]
table['col1'][np.array([0, 2])]

# this slicing and indexing works for ragged array columns as well
table['col4'][:2]  # get a list of the 0th and 1st list elements

###############################################################################
# .. note::
#
#   The syntax ``table[col_name][i]`` is equivalent to ``table[i, col_name]``.

###############################################################################
# Multi-dimensional columns
# -------------------------
# A column can be represented as a multi-dimensional rectangular array or a list of lists, each containing the
# same number of elements.

col5 = VectorData(
    name='col5',
    description='column #5',
    data=[['a', 'b', 'c'], ['d', 'e', 'f'], ['g', 'h', 'i']],
)

###############################################################################
# Ragged multi-dimensional columns
# ---------------------------------
# Each element within a column can be an n-dimensional array or list or lists.
# This is true for ragged array columns as well.

col6 = VectorData(
    name='col6',
    description='column #6',
    data=[['a', 'b', 'c'], ['d', 'e', 'f'], ['g', 'h', 'i']],
)
col6_ind = VectorIndex(
    name='col6_index',
    target=col6,
    data=[2, 3],
)

###############################################################################
# Nested ragged array columns
# ---------------------------
# In the example above, the ragged array column above has two rows. The first row has two elements,
# where each element has 3 sub-elements. This can be thought of as a 2x3 array.
# The second row has one element with 3 sub-elements, or a 1x3 array. This
# works only if the data for ``col5`` is a rectangular array, that is, each row
# element contains the same number of sub-elements. If each row element does
# not contain the same number of sub-elements, then a nested ragged array
# approach must be used instead.
#
# A :py:class:`~hdmf.common.table.VectorIndex` object can index another
# :py:class:`~hdmf.common.table.VectorIndex` object. For example, the first row
# of a table might be a 2x3 array, the second row might be a 3x2 array, and the
# third row might be a 1x1 array. This cannot be represented by a singly
# indexed column, but can be represented by a nested ragged array column.

col7 = VectorData(
    name='col7',
    description='column #6',
    data=['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm'],
)
col7_ind = VectorIndex(
    name='col7_index',
    target=col7,
    data=[3, 6, 8, 10, 12, 13],
)
col7_ind_ind = VectorIndex(
    name='col7_index_index',
    target=col7_ind,
    data=[2, 5, 6],
)

# all indices must be added to the table
table_double_ragged_col = DynamicTable(
    name='my table',
    description='an example table',
    columns=[col7, col7_ind, col7_ind_ind],
)

###############################################################################
# Access the first row using the same syntax as before, except now a list of
# lists is returned. You can then index the resulting list of lists to access
# the individual elements.

table_double_ragged_col[0, 'col7']  # returns [['a', 'b', 'c'], ['d', 'e', 'f']]
table_double_ragged_col['col7'][0]  # same as line above

###############################################################################
# Accessing the column named 'col7' using square bracket notation will return
# the top-level :py:class:`~hdmf.common.table.VectorIndex` for the column.
# Accessing the column named 'col7' using dot notation will return the
# :py:class:`~hdmf.common.table.VectorData` object

table_double_ragged_col['col7']  # returns col7_ind_ind
table_double_ragged_col.col7  # returns the col7 VectorData object

###############################################################################
# Accessing data from a ``DynamicTable`` that contain references to rows of other ``DynamicTable`` objects
# --------------------------------------------------------------------------------------------------------
# By default, when
# :py:meth:`DynamicTable.__getitem__ <hdmf.common.table.DynamicTable.__getitem__>`
# and :py:meth:`DynamicTable.get <hdmf.common.table.DynamicTable.get>` are supplied
# with an int, list of ints, numpy array, or a slice representing rows to return,
# a pandas :py:class:`~pandas.DataFrame` is returned. If the
# :py:class:`~hdmf.common.table.DynamicTable` contains a
# :py:class:`~hdmf.common.table.DynamicTableRegion` column that references rows
# of other ``DynamicTable`` objects, then by default, the
# :py:meth:`DynamicTable.__getitem__ <hdmf.common.table.DynamicTable.__getitem__>`
# and :py:meth:`DynamicTable.get <hdmf.common.table.DynamicTable.get>` methods will
# return row indices of the referenced table, and not the contents of the referenced
# table. To return the contents of the referenced table as a nested
# :py:class:`~pandas.DataFrame` containing only the referenced rows, use
# :py:meth:`DynamicTable.get <hdmf.common.table.DynamicTable.get>` with ``index=False``.

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

# get the first row of the users table
users_table.get(0)

###############################################################################
#

# get the first row of the users table with a nested dataframe
users_table.get(0, index=False)

###############################################################################
#

# get the first two rows of the users table
users_table.get([0, 1])

###############################################################################
#

# get the first two rows of the users table with nested dataframes
# of the addresses table in the address column
users_table.get([0, 1], index=False)

###############################################################################
# .. note::
#   You can also get rows from a :py:class:`~hdmf.common.table.DynamicTable` as a list of
#   lists where the i-th nested list contains the values for the i-th row. This method is
#   generally not recommended.

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
# Creating custom DynamicTable subclasses
# ---------------------------------------
# TODO

###############################################################################
# Defining ``__columns__``
# ^^^^^^^^^^^^^^^^^^^^^^^^
# TODO
