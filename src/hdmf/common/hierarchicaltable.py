"""
Module providing additional functionality for dealing with hierarchically nested tables, i.e.,
tables containing DynamicTableRegion references.
"""
import pandas as pd
from collections import OrderedDict


def get_nested_columns_from_dataframe(df):
    """
    :param df: Input nested DataFrame.
    :type df: pandas.DataFrame

    :returns: List of names of columns containing data frames
    """
    if len(df) > 0:
        return [columnName for (columnName, columnData) in df.iteritems()
                if isinstance(columnData.iloc[0], pd.DataFrame)]
    else:
        return []


def denormalize_nested_dataframe(df, max_depth=None, curr_depth=0):
    """
    Given a nested DataFrame as input, generate a flattened

    :param df: Input nested DataFrame.
    :type df: pandas.DataFrame
    :param max_depth: Maximum recursion depth to be used, i.e., how many levels
                      of nesting should the function resolve
    :type max_depth: unsigned int
    :param curr_depth: Internal parameter used to track the current recursions depth.
                       This should usually not be set by the user.
    :type curr_depth: unsigned int
    """
    nested_cols = get_nested_columns_from_dataframe(df)
    # print(df.name, nested_cols)
    if len(nested_cols) == 0:  # Nothing to do
        # print("END 1")
        # for (columnName, columnData) in df.iteritems():
        #     print(columnName, type(columnData), columnData)
        return df
    # Determine the columns of our denormalized data frame
    col = nested_cols[0]
    res = OrderedDict()
    for ci, c in enumerate(df.columns):
        if c == col:
            # We know we have at least one row because we found nested columns
            # and we also know that our current column is a nested column to this loop is safe
            nested_df = df[c].iloc[0]
            for nc in nested_df.columns:
                res[(df.name, col, nc)] = []
        else:
            res[(df.name, c)] = []
    # print("CurrDepth=%i" % curr_depth, list(res.keys()))

    for row_index in range(len(df)):
        nested_df = df[col].iloc[row_index]
        num_expanded_rows = len(nested_df)
        # Same loop over the columns as above but now we are filling in the data
        for ci, c in enumerate(df.columns):
            if c == col:
                # We know we have at least one row because we found nested columns
                # and we also know that our current column is a nested column to this loop is safe
                if len(nested_df) > 1:
                    for nc in nested_df.columns:
                        res[(df.name, col, nc)] += nested_df[nc].tolist()
                else:
                    for nc in nested_df.columns:
                        res[(df.name, col, nc)] += [nested_df[nc], ]
            else:
                res[(df.name, c)] += ([df[c].iloc[row_index]] * num_expanded_rows)

    # Convert to pandas
    res_df = pd.DataFrame.from_dict(res)
    res_df.name = df.name
    # Recurse if necessary
    if max_depth is not None and curr_depth >= max_depth:  # Stop the recursion and return
        # print("END 2")
        return res_df
    else:  # Continue the recursion
        return denormalize_nested_dataframe(res_df, max_depth=max_depth, curr_depth=(curr_depth+1))
