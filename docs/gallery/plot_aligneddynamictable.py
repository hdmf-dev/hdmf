"""
AlignedDynamicTable
===================

This is a user guide to interacting with ``AlignedDynamicTable`` objects.

"""


###############################################################################
# Introduction
# ------------
#
# The class :py:class:`~hdmf.common.alignedtable.AlignedDynamicTable` represents a column-based table
# with support for grouping columns by category. :py:class:`~hdmf.common.alignedtable.AlignedDynamicTable`
# inherits from :py:class:`~hdmf.common.table.DynamicTable` and may contain additional
# :py:class:`~hdmf.common.table.DynamicTable` objects, one per sub-category. All tables
# must align, i.e., they are required to have the same number of rows. Some key features
# of :py:class:`~hdmf.common.alignedtable.AlignedDynamicTable` are:
#
# * support custom categories, each of which is a :py:class:`~hdmf.common.table.DynamicTable`
#   stored as part of the :py:class:`~hdmf.common.alignedtable.AlignedDynamicTable`,
# * support interaction with category tables individually as well as treating the
#   :py:class:`~hdmf.common.alignedtable.AlignedDynamicTable` as a single large table, and
# * because :py:class:`~hdmf.common.alignedtable.AlignedDynamicTable` is itself a
#   :py:class:`~hdmf.common.table.DynamicTable` users can:
#
#     * Use :py:class:`~hdmf.common.table.DynamicTableRegion` to reference rows in
#       :py:class:`~hdmf.common.alignedtable.AlignedDynamicTable`
#     * Add custom columns to the :py:class:`~hdmf.common.alignedtable.AlignedDynamicTable`, and
#     * Interact with :py:class:`~hdmf.common.alignedtable.AlignedDynamicTable` as well as
#       the category (sub-tables) it contains in the same fashion as with
#       :py:class:`~hdmf.common.table.DynamicTable`
#
# When to use (and not use) AlignedDynamicTable?
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#
# :py:class:`~hdmf.common.alignedtable.AlignedDynamicTable` is a useful data structure but it is also
# fairly complex, consisting of multiple :py:class:`~hdmf.common.table.DynamicTable` objects, each of
# which is itself a complex type composed of many datasets and attributes. In general, if a simpler
# data structure is sufficient, then consider using those instead. For example, consider using instead:
#
# * :py:class:`~hdmf.common.table.DynamicTable` if a regular table is sufficient.
# * A compound dataset via :py:class:`~hdmf.container.Table` if all columns of a table are fixed
#   and fast, column-based access is not critical but fast row-based access is.
# * Multiple, separate tables if using :py:class:`~hdmf.common.alignedtable.AlignedDynamicTable` would
#   lead to duplication of data (i.e., de-normalize data), e.g., by having to replicate values across
#   rows of the table.
#
# Use :py:class:`~hdmf.common.alignedtable.AlignedDynamicTable` when:
#
# * When you need to group columns in a :py:class:`~hdmf.common.table.DynamicTable` by category
# * Need to avoid name collisions between columns in a :py:class:`~hdmf.common.table.DynamicTable`
#   and creating compound columns is not an option
#

###############################################################################
# Constructing a table
# --------------------
#
# To create an :py:class:`~hdmf.common.alignedtable.AlignedDynamicTable`, call the constructor with:
#
# * ``name`` string with the name of the table, and
# * ``description`` string to describe the table.
#

# sphinx_gallery_thumbnail_path = 'figures/gallery_thumbnail_aligneddynamictable.png'
from hdmf.common import AlignedDynamicTable

customer_table = AlignedDynamicTable(
    name='customers',
    description='an example aligned table',
)

###############################################################################
# Initializing columns of the primary table
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#
# The basic behavior of adding data and initializing  :py:class:`~hdmf.common.alignedtable.AlignedDynamicTable`
# is the same as in :py:class:`~hdmf.common.table.DynamicTable`. See the :ref:`dynamictable-howtoguide`
# for details. E.g., using  the ``columns`` and ``colnames`` parameters (which are inherited from
# :py:class:`~hdmf.common.table.DynamicTable`) we can define the columns of the primary table.
# All columns must have the same length.

from hdmf.common import VectorData

col1 = VectorData(
    name='firstname',
    description='Customer first name',
    data=['Peter', 'Emma']
)
col2 = VectorData(
    name='lastname',
    description='Customer last name',
    data=['Williams', 'Brown']
)

customer_table = AlignedDynamicTable(
    name='customer',
    description='an example aligned table',
    columns=[col1, col2]
)

###############################################################################
# Initializing categories
# ^^^^^^^^^^^^^^^^^^^^^^^
#
# By specifying the ``category_tables`` as a list of :py:class:`~hdmf.common.table.DynamicTable`
# objects we can then directly specify the sub-category tables. Optionally, we can also set
# the ``categories`` names of the sub-tables as an array of strings to define the ordering of categories.

from hdmf.common import DynamicTable

# create the home_address category table
subcol1 = VectorData(
    name='city',
    description='city',
    data=['Rivercity', 'Mountaincity']
)
subcol2 = VectorData(
    name='street',
    description='street data',
    data=['Amazonstreet', 'Alpinestreet']
)
homeaddress_table = DynamicTable(
    name='home_address',
    description='home address of the customer',
    columns=[subcol1, subcol2]
)

# create the table
customer_table = AlignedDynamicTable(
    name='customer',
    description='an example aligned table',
    columns=[col1, col2],
    category_tables=[homeaddress_table, ]
)

# render the table in the online docs
customer_table.to_dataframe()

###############################################################################
# Adding more data to the table
# -----------------------------
#
# We can add rows, columns, and new categories to the table.
#
# Adding a row
# ^^^^^^^^^^^^
#
# To add a row via :py:func:`~hdmf.common.alignedtable.AlignedDynamicTable.add_row` we
# can either: 1) provide the row data as a single dict to the ``data`` parameter  or
# 2) specify a dict for each category and column as keyword arguments. Additional
# optional arguments include ``id`` and ``enforce_unique_id``.
#

customer_table.add_row(
    firstname='Paul',
    lastname='Smith',
    home_address={'city': 'Bugcity',
                  'street': 'Beestree'}
)

# render the table in the online docs
customer_table.to_dataframe()

###############################################################################
# Adding a column
# ^^^^^^^^^^^^^^^
#
# To add a columns we use :py:func:`~hdmf.common.alignedtable.AlignedDynamicTable.add_column`.
#

customer_table.add_column(
    name='zipcode',
    description='zip code of the city',
    data=[11111, 22222, 33333],  # specify data for the 3 rows in the table
    category='home_address'  # use None (or omit) to add columns to the primary table
)

# render the table in the online docs
customer_table.to_dataframe()

###############################################################################
# Adding a category
# ^^^^^^^^^^^^^^^^^
#
# To add a new :py:class:`~hdmf.common.table.DynamicTable` as a category,
# we use :py:func:`~hdmf.common.alignedtable.AlignedDynamicTable.add_category`.
#
# .. note::
#    Only regular ``DynamicTables`` are allowed as category tables. Using
#    an ``AlignedDynamicTable`` as a category for another  ``AlignedDynamicTable``
#    is currently not supported.
#

# create a new category DynamicTable for the work address
subcol1 = VectorData(
    name='city',
    description='city',
    data=['Busycity', 'Worktown', 'Labortown']
)
subcol2 = VectorData(
    name='street',
    description='street data',
    data=['Cannery Row', 'Woodwork Avenue', 'Steel Street']
)
subcol3 = VectorData(
    name='zipcode',
    description='zip code of the city',
    data=[33333, 44444, 55555])
workaddress_table = DynamicTable(
    name='work_address',
    description='home address of the customer',
    columns=[subcol1, subcol2, subcol3]
)

# add the category to our AlignedDynamicTable
customer_table.add_category(category=workaddress_table)

# render the table in the online docs
customer_table.to_dataframe()

###############################################################################
# .. note::
#     Because each category is stored as a separate :py:class:`~hdmf.common.table.DynamicTable`
#     there are no name collisions between the columns of the ``home_address`` and ``work_address``
#     tables, so that both can contain matching ``city``, ``street``, and ``zipcode`` columns. However,
#     since a category table is a sub-part of the primary table, categories must not have the
#     same name as other columns or other categories in the primary table.

###############################################################################
# Accessing categories, columns, rows, and cells
# ----------------------------------------------
#
# Convert to a pandas DataFrame
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#
# If we need to access the whole table for analysis, then converting the table
# to pandas DataFrame is a convenient option. To ignore the ``id`` columns of all
# category tables we can simply set the ``ignore_category_ids`` parameter.


# render the table in the online docs while ignoring the id column of category tables
customer_table.to_dataframe(ignore_category_ids=True)

###############################################################################
# Accessing categories
# ^^^^^^^^^^^^^^^^^^^^
#

# Get the list of all categories
_ = customer_table.categories

# Get the DynamicTable object of a particular category
_ = customer_table.get_category(name='home_address')

# Alternatively, we can use normal array slicing to get the category as a pandas DataFrame.
# NOTE: In contrast to the previous call, the table is here converted to a DataFrame.
_ = customer_table['home_address']

###############################################################################
# Accessing columns
# ^^^^^^^^^^^^^^^^^
# We can use the standard Python ``in`` operator to check if a column exists

# To check if a column exists in the primary table we only need to specify the column name
# or alternatively specify the category as None
_ = 'firstname' in customer_table
_ = (None, 'firstname') in customer_table
# To check if a column exists in a category table we need to specify the category
# and column name as a tuple
_ = ('home_address', 'zipcode') in customer_table


###############################################################################
# We can use standard array slicing to get the :py:class:`~hdmf.common.table.VectorData` object of a column.

# To get a column from the primary table we just provide the name.
_ = customer_table['firstname']
# To get a column from a category table we provide both the category name and column name
_ = customer_table['home_address', 'city']


###############################################################################
# Accessing rows
# ^^^^^^^^^^^^^^
#
# Accessing rows works much like in :ref:`dynamictable-howtoguide`
#

# Get a single row by index as a DataFrame
customer_table[1]

###############################################################################
#

# Get a range of rows as a DataFrame
customer_table[0:2]

###############################################################################
#

# Get a list of rows as a DataFrame
customer_table[[0, 2]]


###############################################################################
# Accessing cells
# ^^^^^^^^^^^^^^^
#
# To get a set of cells we need to specify the: 1) category, 2) column, and 3) row index when slicing into the table.
#
# When selecting from the primary table we need to specify None for the category, followed by the column name and
# the selection.

# Select rows 0:2 from the 'firstname' column in the primary table
customer_table[None, 'firstname', 0:2]

###############################################################################
#

# Select rows 1 from the 'firstname' column in the primary table
customer_table[None, 'firstname', 1]

###############################################################################
#

# Select rows 0 and 2 from the 'firstname' column in the primary table
customer_table[None, 'firstname', [0, 2]]

###############################################################################
#

# Select rows 0:2 from the 'city' column of the 'home_address' category table
customer_table['home_address', 'city', 0:2]
