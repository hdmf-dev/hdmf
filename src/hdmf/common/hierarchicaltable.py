"""
Module providing additional functionality for dealing with hierarchically nested tables, i.e.,
tables containing DynamicTableRegion references.
"""
import pandas as pd
import numpy as np
from hdmf.common.table import DynamicTable, DynamicTableRegion, VectorIndex
from hdmf.common.alignedtable import AlignedDynamicTable
from hdmf.utils import docval, getargs


@docval({'name': 'dynamic_table', 'type': DynamicTable,
         'doc': 'DynamicTable object to be converted to a hierarchical pandas.Dataframe'},
        returns="Hierarchical pandas.DataFrame with usually a pandas.MultiIndex on both the index and columns.",
        rtype='pandas.DataFrame',
        is_method=False)
def to_hierarchical_dataframe(dynamic_table):
    """
    Create a hierarchical pandas.DataFrame that represents all data from a collection of linked DynamicTables.

    **LIMITATIONS:** Currently this function only supports DynamicTables with a single DynamicTableRegion column.
    If a table has more than one DynamicTableRegion column then the function will expand only the
    first DynamicTableRegion column found for each table. Any additional DynamicTableRegion columns will remain
    nested.

    **NOTE:** Some useful functions for further processing of the generated
    DataFrame include:

    * pandas.DataFrame.reset_index to turn the data from the pandas.MultiIndex into columns
    * :py:meth:`~hdmf.common.hierarchicaltable.drop_id_columns` to remove all 'id' columns
    * :py:meth:`~hdmf.common.hierarchicaltable.flatten_column_index` to flatten the column index
    """
    # TODO: Need to deal with the case where we have more than one DynamicTableRegion column in a given table
    # Get the references column
    foreign_columns = dynamic_table.get_foreign_columns()
    # if table does not contain any DynamicTableRegion columns then we can just convert it to a dataframe
    if len(foreign_columns) == 0:
        return dynamic_table.to_dataframe()
    hcol_name = foreign_columns[0]   # We only denormalize the first foreign column for now
    hcol = dynamic_table[hcol_name]  # Either a VectorIndex pointing to a DynamicTableRegion or a DynamicTableRegion
    # Get the target DynamicTable that hcol is pointing to. If hcol is a VectorIndex then we first need
    # to get the target of it before we look up the table.
    hcol_target = hcol.table if isinstance(hcol, DynamicTableRegion) else hcol.target.table

    # Create the data variables we need to collect the data for our output dataframe and associated index
    index = []
    data = []
    columns = None
    index_names = None

    #  First we here get a list of DataFrames, one for each row of the column we need to process.
    #  If  hcol is a VectorIndex (i.e., our column is a ragged array of row indices), then simply loading
    #  the data from the VectorIndex will do the trick. If we have a regular DynamicTableRegion column,
    #  then we need to load the elements ourselves (using slice syntax to make sure we get DataFrames)
    #  one-row-at-a-time
    if isinstance(hcol, VectorIndex):
        rows = hcol.get(slice(None), index=False, df=True)
    else:
        rows = [hcol[i:(i+1)] for i in range(len(hcol))]
    # Retrieve the columns we need to iterate over from our input table. For AlignedDynamicTable we need to
    # use the get_colnames function instead of the colnames property to ensure we get all columns not just
    # the columns from the main table
    dynamic_table_colnames = (dynamic_table.get_colnames(include_category_tables=True, ignore_category_ids=False)
                              if isinstance(dynamic_table, AlignedDynamicTable)
                              else dynamic_table.colnames)

    # Case 1:  Our DynamicTableRegion column points to a DynamicTable that itself does not contain
    #          any DynamicTableRegion references (i.e., we have reached the end of our table hierarchy).
    #          If this is the case than we need to de-normalize the data and flatten the hierarchy
    if not hcol_target.has_foreign_columns():
        # Iterate over all rows, where each row is described by a DataFrame with one-or-more rows
        for row_index, row_df in enumerate(rows):
            # Since each row contains a pandas.DataFrame (with possible multiple rows), we
            # next need to iterate over all rows in that table to denormalize our data
            for row in row_df.itertuples(index=True):
                # Determine the column data for our row. Each selected row from our target table
                # becomes a row in our flattened table
                data.append(row)
                #  Determine the multi-index tuple for our row, consisting of: i) id of the row in this
                #  table, ii) all columns (except the hierarchical column we are flattening), and
                #  iii) the index (i.e., id) from our target row
                index_data = ([dynamic_table.id[row_index], ] +
                              [dynamic_table[row_index, colname]
                               for colname in dynamic_table_colnames if colname != hcol_name])
                index.append(tuple(index_data))

        # Determine the names for our index and columns of our output table
        # We need to do this even if our table was empty (i.e. even is len(rows)==0)
        # NOTE: While for a regular DynamicTable the "colnames" property will give us the full list of column names,
        #       for AlignedDynamicTable we need to use the get_colnames() function instead to make sure we include
        #       the category table columns as well.
        index_names = ([(dynamic_table.name, 'id')] +
                       [(dynamic_table.name, colname)
                        for colname in dynamic_table_colnames if colname != hcol_name])
        # Determine the name of our columns
        hcol_iter_columns = (hcol_target.get_colnames(include_category_tables=True, ignore_category_ids=False)
                             if isinstance(hcol_target, AlignedDynamicTable)
                             else hcol_target.colnames)
        columns = pd.MultiIndex.from_tuples([(hcol_target.name, 'id'), ] +
                                            [(hcol_target.name, c) for c in hcol_iter_columns],
                                            names=('source_table', 'label'))

    # Case 2:  Our DynamicTableRegion columns points to another table with a DynamicTableRegion, i.e.,
    #          we need to recursively resolve more levels of the table hierarchy
    else:
        # First we need to recursively flatten the hierarchy by calling 'to_hierarchical_dataframe()'
        # (i.e., this function) on the target of our hierarchical column
        hcol_hdf = to_hierarchical_dataframe(hcol_target)
        # Iterate over all rows, where each row is described by a DataFrame with one-or-more rows
        for row_index, row_df_level1 in enumerate(rows):
            # Since each row contains a pandas.DataFrame (with possible multiple rows), we
            # next need to iterate over all rows in that table to denormalize our data
            for row_df_level2 in row_df_level1.itertuples(index=True):
                # Since our target is itself a a DynamicTable with a DynamicTableRegion columns,
                # each target row itself may expand into multiple rows in the flattened hcol_hdf.
                # So we now need to look up the rows in hcol_hdf that correspond to the rows in
                # row_df_level2.
                # NOTE: In this look-up we assume that the ids (and hence the index) of
                #       each row in the table are in fact unique.
                for row_tuple_level3 in hcol_hdf.loc[[row_df_level2[0]]].itertuples(index=True):
                    # Determine the column data for our row.
                    data.append(row_tuple_level3[1:])
                    # Determine the multi-index tuple for our row,
                    index_data = ([dynamic_table.id[row_index], ] +
                                  [dynamic_table[row_index, colname]
                                   for colname in dynamic_table_colnames if colname != hcol_name] +
                                  list(row_tuple_level3[0]))
                    index.append(tuple(index_data))
        # Determine the names for our index and columns of our output table
        # We need to do this even if our table was empty (i.e. even is len(rows)==0)
        index_names = ([(dynamic_table.name, "id")] +
                       [(dynamic_table.name, colname)
                        for colname in dynamic_table_colnames if colname != hcol_name] +
                       hcol_hdf.index.names)
        columns = hcol_hdf.columns

    # Check if the index contains any unhashable types. If a table contains a VectorIndex column
    # (other than the DynamicTableRegion column) then "TypeError: unhashable type: 'list'" will
    # occur when converting the index to pd.MultiIndex. To avoid this error, we next check if any
    # of the columns in our index are of type list or np.ndarray
    unhashable_index_cols = []
    if len(index) > 0:
        unhashable_index_cols = [i for i, v in enumerate(index[0]) if isinstance(v, (list, np.ndarray))]

    # If we have any unhashable list or np.array objects in the index then update them to tuples.
    # Ideally we would detect this case when constructing the index, but it is easier to do this
    # here and it should not be much more expensive, but it requires iterating over all rows again
    if len(unhashable_index_cols) > 0:
        for i, v in enumerate(index):
            temp = list(v)
            for ci in unhashable_index_cols:
                temp[ci] = tuple(temp[ci])
            index[i] = tuple(temp)

    # Construct the pandas dataframe with the hierarchical multi-index
    multi_index = pd.MultiIndex.from_tuples(index, names=index_names)
    out_df = pd.DataFrame(data=data, index=multi_index, columns=columns)
    return out_df


def __get_col_name(col):
    """
    Internal helper function to get the actual name of a pandas DataFrame column from a
    column name that may consists of an arbitrary sequence of tuples. The function
    will return the last value of the innermost tuple.
    """
    curr_val = col
    while isinstance(curr_val, tuple):
        curr_val = curr_val[-1]
    return curr_val


def __flatten_column_name(col):
    """
    Internal helper function used to iteratively flatten a nested tuple

    :param col: Column name to flatten
    :type col: Tuple or String

    :returns: If col is a tuple then the result is a flat tuple otherwise col is returned as is
    """
    if isinstance(col, tuple):
        re = col
        while np.any([isinstance(v, tuple) for v in re]):
            temp = []
            for v in re:
                if isinstance(v, tuple):
                    temp += list(v)
                else:
                    temp += [v, ]
            re = temp
        return tuple(re)
    else:
        return col


@docval({'name': 'dataframe', 'type': pd.DataFrame,
         'doc': 'Pandas dataframe to update (usually generated by the to_hierarchical_dataframe function)'},
        {'name': 'inplace', 'type': 'bool', 'doc': 'Update the dataframe inplace or return a modified copy',
         'default': False},
        returns="pandas.DataFrame with the id columns removed",
        rtype='pandas.DataFrame',
        is_method=False)
def drop_id_columns(**kwargs):
    """
    Drop all columns named 'id' from the table.

    In case a column name is a tuple the function will drop any column for which
    the inner-most name is 'id'. The 'id' columns of DynamicTable is in many cases
    not necessary for analysis or display. This function allow us to easily filter
    all those columns.

    :raises TypeError: In case that dataframe parameter is not a pandas.Dataframe.
    """
    dataframe, inplace = getargs('dataframe', 'inplace', kwargs)
    col_name = 'id'
    drop_labels = []
    for col in dataframe.columns:
        if __get_col_name(col) == col_name:
            drop_labels.append(col)
    re = dataframe.drop(labels=drop_labels, axis=1, inplace=inplace)
    return dataframe if inplace else re


@docval({'name': 'dataframe', 'type': pd.DataFrame,
         'doc': 'Pandas dataframe to update (usually generated by the to_hierarchical_dataframe function)'},
        {'name': 'max_levels', 'type': (int, np.integer),
         'doc': 'Maximum number of levels to use in the resulting column Index. NOTE:  When '
                'limiting the number of levels the function simply removes levels from the '
                'beginning. As such, removing levels may result in columns with duplicate names.'
                'Value must be >0.',
         'default': None},
        {'name': 'inplace', 'type': 'bool', 'doc': 'Update the dataframe inplace or return a modified copy',
         'default': False},
        returns="pandas.DataFrame with a regular pandas.Index columns rather and a pandas.MultiIndex",
        rtype='pandas.DataFrame',
        is_method=False)
def flatten_column_index(**kwargs):
    """
    Flatten the column index of a pandas DataFrame.

    The functions changes the dataframe.columns from a pandas.MultiIndex to a normal Index,
    with each column usually being identified by a tuple of strings. This function is
    typically used in conjunction with DataFrames generated
    by :py:meth:`~hdmf.common.hierarchicaltable.to_hierarchical_dataframe`

    :raises ValueError: In case the num_levels is not >0
    :raises TypeError: In case that dataframe parameter is not a pandas.Dataframe.
    """
    dataframe, max_levels, inplace = getargs('dataframe', 'max_levels', 'inplace', kwargs)
    if max_levels is not None and max_levels <= 0:
        raise ValueError('max_levels must be greater than 0')
    # Compute the new column names
    col_names = [__flatten_column_name(col) for col in dataframe.columns.values]
    # Apply the max_levels filter. Make sure to do this only for columns that are actually tuples
    # in order not to accidentally shorten the actual string name of columns
    if max_levels is None:
        select_levels = slice(None)
    elif max_levels == 1:
        select_levels = -1
    else:  # max_levels > 1
        select_levels = slice(-max_levels, None)
    col_names = [col[select_levels] if isinstance(col, tuple) else col for col in col_names]
    re = dataframe if inplace else dataframe.copy()
    re.columns = col_names
    return re
