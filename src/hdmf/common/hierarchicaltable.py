"""
Module providing additional functionality for dealing with hierarchically nested tables, i.e.,
tables containing DynamicTableRegion references.
"""
import pandas as pd
import numpy as np
from hdmf.common.table import DynamicTableRegion, VectorIndex
from collections import OrderedDict


# TODO: Need to deal with AlignedDynamicTable
# TODO: Check flat_column_index functionality (what it really does and if it is working)


def to_hierarchical_dataframe(dynamic_table, flat_column_index=False):
    """
    Create a Pandas dataframe with a hierarchical MultiIndex index that represents the
    hierarchical dynamic table.
    """
    # Get the references column
    foreign_columns = dynamic_table.get_foreign_columns()
    if len(foreign_columns) == 0:
        return dynamic_table.to_dataframe()
    hcol_name = foreign_columns[0]  # We only denormalize the first foreign column for now
    hcol = dynamic_table[hcol_name]
    # If our foreign column is a vectorindex
    hcol_target = hcol.table if isinstance(hcol, DynamicTableRegion) else hcol.target.table

    # Create the data variables we need to collect the data for our output dataframe and associated index
    index = []
    data = []
    columns = None
    index_names = None

    # If we have indexed columns (other than our hierarchical column) then our index data for our
    # MultiIndex will contain lists as elements (which are not hashable) and as such create an error.
    # As such we need to check if we have any affected columns so we can  fix our data
    indexed_column_indicies = np.where([isinstance(dynamic_table[colname], VectorIndex)
                                        for colname in dynamic_table.colnames if colname != hcol_name])[0]
    indexed_column_indicies += 1  # Need to increment by 1 since we add the row id in our iteration below

    # Case 1:  Our DynamicTableRegion column points to a regular DynamicTable
    #          If this is the case than we need to de-normalize the data and flatten the hierarchy
    if not hcol_target.has_foreign_columns():
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
                index_data = ([dynamic_table.id[row_index], ] +
                              [dynamic_table[row_index, colname] for colname in dynamic_table.colnames if colname != hcol_name])
                for i in indexed_column_indicies:  # Fix data from indexed columns
                    index_data[i] = tuple(index_data[i])  # Convert from list to tuple (which is hashable)
                index.append(tuple(index_data))
                # Determine the names for our index and columns of our output table if this is the first row.
                # These are constant for all rows so we only need to do this onle once for the first row.
                if row_index == 0:
                    index_names = ([(dynamic_table.name, 'id')] +
                                   [(dynamic_table.name, colname)
                                    for colname in dynamic_table.colnames if colname != hcol_name])
                    if flat_column_index:
                        columns = [(hcol_target.name, 'id'), ] + list(row_df.columns)
                    else:
                        columns = pd.MultiIndex.from_tuples([(hcol_target.name, 'id'), ] +
                                                            [(hcol_target.name, c) for c in row_df.columns],
                                                            names=('source_table', 'label'))
        #  if we had an empty data table then at least define the columns
        if index_names is None:
            index_names = ([(dynamic_table.name, 'id')] +
                           [(dynamic_table.name, colname)
                            for colname in dynamic_table.colnames if colname != hcol_name])
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
        hcol_hdf = to_hierarchical_dataframe(hcol_target, flat_column_index=flat_column_index)
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
                    index_data = ([dynamic_table.id[row_index], ] +
                                  [dynamic_table[row_index, colname] for colname in dynamic_table.colnames if colname != hcol_name] +
                                  list(row_tuple_level3[0]))
                    for i in indexed_column_indicies:  # Fix data from indexed columns
                        index_data[i] = tuple(index_data[i])  # Convert from list to tuple (which is hashable)
                    index.append(tuple(index_data))
                    # Determine the names for our index and columns of our output table if this is the first row
                    if row_index == 0:
                        index_names = ([(dynamic_table.name, "id")] +
                                       [(dynamic_table.name, colname)
                                        for colname in dynamic_table.colnames if colname != hcol_name] +
                                       hcol_hdf.index.names)
                        columns = hcol_hdf.columns
        # if we had an empty table, then at least define the columns
        if index_names is None:
            index_names = ([(dynamic_table.name, "id")] +
                           [(dynamic_table.name, colname)
                            for colname in dynamic_table.colnames if colname != hcol_name] +
                           hcol_hdf.index.names)
            columns = hcol_hdf.columns

    # Construct the pandas dataframe with the hierarchical multi-index
    multi_index = pd.MultiIndex.from_tuples(index, names=index_names)
    out_df = pd.DataFrame(data=data, index=multi_index, columns=columns)
    return out_df


def to_denormalized_dataframe(dynamic_table, flat_column_index=False):
    """
    Shorthand for 'self.to_hierarchical_dataframe().reset_index()'

    The function denormalizes the hierarchical table and represents all data as
    columns in the resulting dataframe.
    """
    hier_df = to_hierarchical_dataframe(dynamic_table=dynamic_table, flat_column_index=False)
    flat_df = hier_df.reset_index()
    if not flat_column_index:
        # cn[0] is the level, cn[1:] is the label. If cn has only 2 elements than use cn[1] instead to
        # avoid creating column labels that are tuples with just one element
        mi_tuples = [(cn[0], cn[1:] if len(cn) > 2 else cn[1])
                     for cn in flat_df.columns]
        flat_df.columns = pd.MultiIndex.from_tuples(mi_tuples, names=('source_table', 'label'))

    return flat_df