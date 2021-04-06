"""
Collection of Container classes for interacting with  aligned and hierarchical dynamic tables
"""
from collections import OrderedDict

import numpy as np
import pandas as pd

from . import register_class
from .table import DynamicTable, DynamicTableRegion, VectorIndex
from ..utils import docval, getargs, call_docval_func, popargs, get_docval


@register_class('AlignedDynamicTable')
class AlignedDynamicTable(DynamicTable):
    """
    DynamicTable container that supports storing a collection of subtables. Each sub-table is a
    DynamicTable itself that is aligned with the main table by row index. I.e., all
    DynamicTables stored in this group MUST have the same number of rows. This type effectively
    defines a 2-level table in which the main data is stored in the main table implemented by this type
    and additional columns of the table are grouped into categories, with each category being'
    represented by a separate DynamicTable stored within the group.
    """
    __fields__ = ({'name': 'category_tables', 'child': True}, )

    @docval(*get_docval(DynamicTable.__init__),
            {'name': 'category_tables', 'type': list,
             'doc': 'List of DynamicTables to be added to the container', 'default': None},
            {'name': 'categories', 'type': 'array_data',
             'doc': 'List of names with the ordering of category tables', 'default': None})
    def __init__(self, **kwargs):
        in_category_tables = popargs('category_tables', kwargs)
        in_categories = popargs('categories', kwargs)
        if in_categories is None and in_category_tables is not None:
            in_categories = [tab.name for tab in in_category_tables]
        if in_categories is not None and in_category_tables is None:
            raise ValueError("Categories provided but no category_tables given")
        # at this point both in_categories and in_category_tables should either both be None or both be a list
        if in_categories is not None:
            if len(in_categories) != len(in_category_tables):
                raise ValueError("%s category_tables given but %s categories specified" %
                                 (len(in_category_tables), len(in_categories)))
        # Initialize the main dynamic table
        call_docval_func(super().__init__, kwargs)
        # Create and set all sub-categories
        dts = OrderedDict()
        # Add the custom categories given as inputs
        if in_category_tables is not None:
            # We may need to resize our main table when adding categories as the user may not have set ids
            if len(in_category_tables) > 0:
                # We have categories to process
                if len(self.id) == 0:
                    # The user did not initialize our main table id's nor set columns for our main table
                    for i in range(len(in_category_tables[0])):
                        self.id.append(i)
            # Add the user-provided categories in the correct order as described by the categories
            # This is necessary, because we do not store the categories explicitly but we maintain them
            # as the order of our self.category_tables. In this makes sure look-ups are consistent.
            lookup_index = OrderedDict([(k, -1) for k in in_categories])
            for i, v in enumerate(in_category_tables):
                # Error check that the name of the table is in our categories list
                if v.name not in lookup_index:
                    raise ValueError("DynamicTable %s does not appear in categories %s" % (v.name, str(in_categories)))
                # Error check to make sure no two tables with the same name are given
                if lookup_index[v.name] >= 0:
                    raise ValueError("Duplicate table name %s found in input dynamic_tables" % v.name)
                lookup_index[v.name] = i
            for table_name, tabel_index in lookup_index.items():
                # This error case should not be able to occur since the length of the in_categories and
                # in_category_tables must match and we made sure that each DynamicTable we added had its
                # name in the in_categories list. We, therefore, exclude this check from coverage testing
                # but we leave it in just as a backup trigger in case something unexpected happens
                if tabel_index < 0:  # pragma: no cover
                    raise ValueError("DynamicTable %s listed in categories but does not appear in category_tables" %
                                     table_name)  # pragma: no cover
                # Test that all category tables have the correct number of rows
                category = in_category_tables[tabel_index]
                if len(category) != len(self):
                    raise ValueError('Category DynamicTable %s does not align, it has %i rows expected %i' %
                                     (category.name, len(category), len(self)))
                # Add the category table to our category_tables.
                dts[category.name] = category
        # Set the self.category_tables attribute, which will set the parent/child relationships for the category_tables
        self.category_tables = dts

    def __contains__(self, val):
        """
        Check if the given value (i.e., column) exists in this table

        :param val: If val is a string then check if the given category exists. If val is a tuple
        of two strings (category, colname) then check for the given category if the given colname exists.
        """
        if isinstance(val, str):
            return val in self.category_tables or val in self.colnames
        elif isinstance(val, tuple):
            if len(val) != 2:
                raise ValueError("Expected tuple of strings of length 2 got tuple of length %i" % len(val))
            return val[1] in self.get_category(val[0])
        else:
            return False

    @property
    def categories(self):
        """
        Get the list of names the categories

        Short-hand for list(self.category_tables.keys())

        :raises: KeyError if the given name is not in self.category_tables
        """
        return list(self.category_tables.keys())

    @docval({'name': 'category', 'type': DynamicTable, 'doc': 'Add a new DynamicTable category'},)
    def add_category(self, **kwargs):
        """
        Add a new DynamicTable to the AlignedDynamicTable to create a new category in the table.

        NOTE: The table must align with (i.e, have the same number of rows as) the main data table (and
        other category tables). I.e., if the AlignedDynamicTable is already populated with data
        then we have to populate the new category with the corresponding data before adding it.

        :raises: ValueError is raised if the input table does not have the same number of rows as the main table
        """
        category = getargs('category', kwargs)
        if len(category) != len(self):
            raise ValueError('New category DynamicTable does not align, it has %i rows expected %i' %
                             (len(category), len(self)))
        if category.name in self.category_tables:
            raise ValueError("Category %s already in the table" % category.name)
        self.category_tables[category.name] = category
        category.parent = self

    @docval({'name': 'name', 'type': str, 'doc': 'Name of the category we want to retrieve', 'default': None})
    def get_category(self, **kwargs):
        name = popargs('name', kwargs)
        if name is None or (name not in self.category_tables and name == self.name):
            return self
        else:
            return self.category_tables[name]

    @docval(*get_docval(DynamicTable.add_column),
            {'name': 'category', 'type': str, 'doc': 'The category the column should be added to',
             'default': None})
    def add_column(self, **kwargs):
        """
        Add a column to the table

        :raises: KeyError if the category does not exist

        """
        category_name = popargs('category', kwargs)
        if category_name is None:
            # Add the column to our main table
            call_docval_func(super().add_column, kwargs)
        else:
            # Add the column to a sub-category table
            try:
                category = self.get_category(category_name)
            except KeyError:
                raise KeyError("Category %s not in table" % category_name)
            category.add_column(**kwargs)

    @docval({'name': 'data', 'type': dict, 'doc': 'the data to put in this row', 'default': None},
            {'name': 'id', 'type': int, 'doc': 'the ID for the row', 'default': None},
            {'name': 'enforce_unique_id', 'type': bool, 'doc': 'enforce that the id in the table must be unique',
             'default': False},
            allow_extra=True)
    def add_row(self, **kwargs):
        """
        We can either provide the row data as a single dict or by specifying a dict for each category
        """
        data, row_id, enforce_unique_id = popargs('data', 'id', 'enforce_unique_id', kwargs)
        data = data if data is not None else kwargs

        # extract the category data
        category_data = {k: data.pop(k) for k in self.categories if k in data}

        # Check that we have the approbriate categories provided
        missing_categories = set(self.categories) - set(list(category_data.keys()))
        if missing_categories:
            raise KeyError(
                '\n'.join([
                    'row data keys don\'t match available categories',
                    'missing {} category keys: {}'.format(len(missing_categories), missing_categories)
                ])
            )
        # Add the data to our main dynamic table
        data['id'] = row_id
        data['enforce_unique_id'] = enforce_unique_id
        call_docval_func(super().add_row, data)

        # Add the data to all out dynamic table categories
        for category, values in category_data.items():
            self.category_tables[category].add_row(**values)

    @docval({'name': 'ignore_category_ids', 'type': bool,
             'doc': "Ignore id columns of sub-category tables", 'default': False})
    def to_dataframe(self, **kwargs):
        """Convert the collection of tables to a single pandas DataFrame"""
        dfs = [super().to_dataframe().reset_index(), ]
        if getargs('ignore_category_ids', kwargs):
            dfs += [category.to_dataframe() for category in self.category_tables.values()]
        else:
            dfs += [category.to_dataframe().reset_index() for category in self.category_tables.values()]
        names = [self.name, ] + list(self.category_tables.keys())
        res = pd.concat(dfs, axis=1, keys=names)
        res.set_index((self.name, 'id'), drop=True, inplace=True)
        return res

    def __getitem__(self, item):
        """
        If item is:
        * int : Return a single row of the table
        * string : Return a single category of the table
        * tuple: Get a column, row, or cell from a particular category. The tuple is expected to consist
                 of (category, selection) where category may be a string with the name of the sub-category
                 or None (or the name of this AlignedDynamicTable) if we want to slice into the main table.

        :returns: DataFrame when retrieving a row or category. Returns scalar when selecting a cell.
                 Returns a VectorData/VectorIndex when retrieving a single column.
        """
        if isinstance(item, (int, list, np.ndarray, slice)):
            # get a single full row from all tables
            dfs = ([super().__getitem__(item).reset_index(), ] +
                   [category[item].reset_index() for category in self.category_tables.values()])
            names = [self.name, ] + list(self.category_tables.keys())
            res = pd.concat(dfs, axis=1, keys=names)
            res.set_index((self.name, 'id'), drop=True, inplace=True)
            return res
        elif isinstance(item, str) or item is None:
            if item in self.colnames:
                # get a specfic column
                return super().__getitem__(item)
            else:
                # get a single category
                return self.get_category(item).to_dataframe()
        elif isinstance(item, tuple):
            if len(item) == 2:
                return self.get_category(item[0])[item[1]]
            elif len(item) == 3:
                return self.get_category(item[0])[item[1]][item[2]]
            else:
                raise ValueError("Expected tuple of length 2 or 3 with (category, column, row) as value.")


class HierarchicalDynamicTableMixin:
    """
    Mixin class for defining specialized functionality for hierarchical dynamic tables.

    Assumptions:

    1) The current implementation assumes that there is only one DynamicTableRegion column
    that needs to be expanded as part of the hierarchy.  Allowing multiple hierarchical
    columns in a single table get tricky, because it is unclear how those rows should
    be joined. To clarify, allowing multiple DynamicTableRegion should be fine, as long
    as only one of them should be expanded as part of the hierarchy.

    2) The default implementation of the get_hierarchy_column_name function assumes that
    the first DynamicTableRegion that references a DynamicTable that inherits from
    HierarchicalDynamicTableMixin is the one that should be expanded as part of the
    hierarchy of tables. If there is no such column, then the default implementation
    assumes that the first DynamicTableRegion column is the one that needs to be expanded.
    These assumption of get_hierarchy_column_name can be easily fixed by overwriting
    the function in the subclass to return the name of the approbritate column.
    """

    def get_hierarchy_column_name(self):
        """
        Get the name of column that references another DynamicTable that
        is itself a HierarchicalDynamicTableMixin table.

        :returns: String with the column name or None
        """
        first_col = None
        for col_index, col in enumerate(self.columns):
            if isinstance(col, DynamicTableRegion):
                first_col = col.name
                if isinstance(col.table, HierarchicalDynamicTableMixin):
                    return col.name
        return first_col

    def get_referencing_column_names(self):
        """
        Determine the names of all columns that reference another table, i.e.,
        find all DynamicTableRegion type columns

        Returns: List of strings with the column names
        """
        col_names = []
        for col_index, col in enumerate(self.columns):
            if isinstance(col, DynamicTableRegion):
                col_names.append(col.name)
        return col_names

    def get_targets(self, include_self=False):
        """
        Get a list of the full table hierarchy, i.e., recursively list all
        tables referenced in the hierarchy.

        Returns: List of DynamicTable objects

        """
        hcol_name = self.get_hierarchy_column_name()
        hcol = self[hcol_name]
        hcol_target = hcol.table if isinstance(hcol, DynamicTableRegion) else hcol.target.table
        if isinstance(hcol_target, HierarchicalDynamicTableMixin):
            re = [self, ] if include_self else []
            re += [hcol_target, ]
            re += hcol_target.get_targets()
            return re
        else:
            return [hcol_target, ]

    def to_denormalized_dataframe(self, flat_column_index=False):
        """
        Shorthand for 'self.to_hierarchical_dataframe().reset_index()'

        The function denormalizes the hierarchical table and represents all data as
        columns in the resulting dataframe.
        """
        hier_df = self.to_hierarchical_dataframe(flat_column_index=True)
        flat_df = hier_df.reset_index()
        if not flat_column_index:
            # cn[0] is the level, cn[1:] is the label. If cn has only 2 elements than use cn[1] instead to
            # avoid creating column labels that are tuples with just one element
            mi_tuples = [(cn[0], cn[1:] if len(cn) > 2 else cn[1])
                         for cn in flat_df.columns]
            flat_df.columns = pd.MultiIndex.from_tuples(mi_tuples, names=('source_table', 'label'))

        return flat_df

    def to_hierarchical_dataframe(self, flat_column_index=False):
        """
        Create a Pandas dataframe with a hierarchical MultiIndex index that represents the
        hierarchical dynamic table.
        """
        # Get the references column
        hcol_name = self.get_hierarchy_column_name()
        hcol = self[hcol_name]
        hcol_target = hcol.table if isinstance(hcol, DynamicTableRegion) else hcol.target.table

        # Create the data variables we need to collect the data for our output dataframe and associated index
        index = []
        data = []
        columns = None
        index_names = None

        # If we have indexed columns (other than our hierarchical column) then our index data for our
        # MultiIndex will contain lists as elements (which are not hashable) and as such create an error.
        # As such we need to check if we have any affected columns so we can  fix our data
        indexed_column_indicies = np.where([isinstance(self[colname], VectorIndex)
                                            for colname in self.colnames if colname != hcol_name])[0]
        indexed_column_indicies += 1  # Need to increment by 1 since we add the row id in our iteration below

        # Case 1:  Our DynamicTableRegion column points to a regular DynamicTable
        #          If this is the case than we need to de-normalize the data and flatten the hierarchy
        if not isinstance(hcol_target, HierarchicalDynamicTableMixin):
            # 1) Iterate over all rows in our hierarchical columns (i.e,. the DynamicTableRegion column)
            for row_index, row_df in enumerate(hcol[:]):  # need hcol[:] here in case this is an h5py.Dataset
                # 1.1): Since hcol is a DynamicTableRegion, each row returns another DynamicTable so we
                #       next need to iterate over all rows in that table to denormalize our data
                for row in row_df.itertuples(index=True):
                    # 1.1.1) Determine the column data for our row. Each selected row from our target table
                    #        becomes a row in our flattened table
                    data.append(row)
                    # 1.1.2) Determine the multi-index tuple for our row, consisting of: i) id of the row in this
                    #        table, ii) all columns (except the hierarchical column we are flattening), and
                    #        iii) the index (i.e., id) from our target row
                    index_data = ([self.id[row_index], ] +
                                  [self[row_index, colname] for colname in self.colnames if colname != hcol_name])
                    for i in indexed_column_indicies:  # Fix data from indexed columns
                        index_data[i] = tuple(index_data[i])  # Convert from list to tuple (which is hashable)
                    index.append(tuple(index_data))
                    # Determine the names for our index and columns of our output table if this is the first row.
                    # These are constant for all rows so we only need to do this onle once for the first row.
                    if row_index == 0:
                        index_names = ([(self.name, 'id')] +
                                       [(self.name, colname)
                                        for colname in self.colnames if colname != hcol_name])
                        if flat_column_index:
                            columns = [(hcol_target.name, 'id'), ] + list(row_df.columns)
                        else:
                            columns = pd.MultiIndex.from_tuples([(hcol_target.name, 'id'), ] +
                                                                [(hcol_target.name, c) for c in row_df.columns],
                                                                names=('source_table', 'label'))
            #  if we had an empty data table then at least define the columns
            if index_names is None:
                index_names = ([(self.name, 'id')] +
                               [(self.name, colname)
                                for colname in self.colnames if colname != hcol_name])
                if flat_column_index:
                    columns = [(hcol_target.name, 'id'), ] + list(hcol_target.colnames)
                else:
                    columns = pd.MultiIndex.from_tuples([(hcol_target.name, 'id'), ] +
                                                        [(hcol_target.name, c) for c in hcol_target.colnames],
                                                        names=('source_table', 'label'))

        # Case 2:  Our DynamicTableRegion columns points to another HierarchicalDynamicTable.
        else:
            # 1) First we need to recursively flatten the hierarchy by calling 'to_hierarchical_dataframe()'
            #    (i.e., this function) on the target of our hierarchical column
            hcol_hdf = hcol_target.to_hierarchical_dataframe(flat_column_index=flat_column_index)
            # 2) Iterate over all rows in our hierarchcial columns (i.e,. the DynamicTableRegion column)
            for row_index, row_df_level1 in enumerate(hcol[:]):   # need hcol[:] here  in case this is an h5py.Dataset
                # 1.1): Since hcol is a DynamicTableRegion, each row returns another DynamicTable so we
                #       next need to iterate over all rows in that table to denormalize our data
                for row_df_level2 in row_df_level1.itertuples(index=True):
                    # 1.1.2) Since our target is itself a HierarchicalDynamicTable each target row itself
                    #        may expand into multiple rows in flattened hcol_hdf. So we now need to look
                    #        up the rows in hcol_hdf that correspond to the rows in row_df_level2.
                    #        NOTE: In this look-up we assume that the ids (and hence the index) of
                    #              each row in the table are in fact unique.
                    for row_tuple_level3 in hcol_hdf.loc[[row_df_level2[0]]].itertuples(index=True):
                        # 1.1.2.1) Determine the column data for our row.
                        data.append(row_tuple_level3[1:])
                        # 1.1.2.2) Determine the multi-index tuple for our row,
                        index_data = ([self.id[row_index], ] +
                                      [self[row_index, colname] for colname in self.colnames if colname != hcol_name] +
                                      list(row_tuple_level3[0]))
                        for i in indexed_column_indicies:  # Fix data from indexed columns
                            index_data[i] = tuple(index_data[i])  # Convert from list to tuple (which is hashable)
                        index.append(tuple(index_data))
                        # Determine the names for our index and columns of our output table if this is the first row
                        if row_index == 0:
                            index_names = ([(self.name, "id")] +
                                           [(self.name, colname)
                                            for colname in self.colnames if colname != hcol_name] +
                                           hcol_hdf.index.names)
                            columns = hcol_hdf.columns
            # if we had an empty table, then at least define the columns
            if index_names is None:
                index_names = ([(self.name, "id")] +
                               [(self.name, colname)
                                for colname in self.colnames if colname != hcol_name] +
                               hcol_hdf.index.names)
                columns = hcol_hdf.columns

        # Construct the pandas dataframe with the hierarchical multi-index
        multi_index = pd.MultiIndex.from_tuples(index, names=index_names)
        out_df = pd.DataFrame(data=data, index=multi_index, columns=columns)
        return out_df
