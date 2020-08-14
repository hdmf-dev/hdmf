"""
Collection of Container classes for interacting with data types related to
the storage and use of dynamic data tables as part of the hdmf-common schema
"""

from h5py import Dataset
import numpy as np
import pandas as pd
import re
from collections import OrderedDict
from warnings import warn

from ..utils import docval, getargs, ExtenderMeta, call_docval_func, popargs, pystr
from ..data_utils import DataIO, AbstractDataChunkIterator
from ..container import Container, Data

from . import register_class


@register_class('VectorData')
class VectorData(Data):
    """
    A n-dimensional dataset representing a column of a DynamicTable.
    If used without an accompanying VectorIndex, first dimension is
    along the rows of the DynamicTable and each step along the first
    dimension is a cell of the larger table. VectorData can also be
    used to represent a ragged array if paired with a VectorIndex.
    This allows for storing arrays of varying length in a single cell
    of the DynamicTable by indexing into this VectorData. The first
    vector is at VectorData[0:VectorIndex(0)+1]. The second vector is at
    VectorData[VectorIndex(0)+1:VectorIndex(1)+1], and so on.
    """

    __fields__ = ("description",)

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this VectorData'},
            {'name': 'description', 'type': str, 'doc': 'a description for this column'},
            {'name': 'data', 'type': ('array_data', 'data'),
             'doc': 'a dataset where the first dimension is a concatenation of multiple vectors', 'default': list()})
    def __init__(self, **kwargs):
        call_docval_func(super().__init__, kwargs)
        self.description = getargs('description', kwargs)

    @docval({'name': 'val', 'type': None, 'doc': 'the value to add to this column'})
    def add_row(self, **kwargs):
        """Append a data value to this VectorData column"""
        val = getargs('val', kwargs)
        self.append(val)

    def get(self, key, **kwargs):
        return super().get(key)


@register_class('VectorIndex')
class VectorIndex(VectorData):
    """
    When paired with a VectorData, this allows for storing arrays of varying
    length in a single cell of the DynamicTable by indexing into this VectorData.
    The first vector is at VectorData[0:VectorIndex(0)+1]. The second vector is at
    VectorData[VectorIndex(0)+1:VectorIndex(1)+1], and so on.
    """

    __fields__ = ("target",)

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this VectorIndex'},
            {'name': 'data', 'type': ('array_data', 'data'),
             'doc': 'a 1D dataset containing indexes that apply to VectorData object'},
            {'name': 'target', 'type': VectorData,
             'doc': 'the target dataset that this index applies to'})
    def __init__(self, **kwargs):
        target = getargs('target', kwargs)
        kwargs['description'] = "Index for VectorData '%s'" % target.name
        call_docval_func(super().__init__, kwargs)
        self.target = target
        self.__uint = np.uint8
        self.__maxval = 255
        if isinstance(self.data, (list, np.ndarray)):
            if len(self.data) > 0:
                self.__check_precision(len(self.target))
            # adjust precision for types that we can adjust precision for
            self.__adjust_precision(self.__uint)

    def add_vector(self, arg):
        """
        Add the given data value to the target VectorData and append the corresponding index to this VectorIndex
        :param arg: The data value to be added to self.target
        """
        self.target.extend(arg)
        self.append(self.__check_precision(len(self.target)))

    def __check_precision(self, idx):
        """
        Check precision of current dataset and, if
        necessary, adjust precision to accomodate new value.

        Returns:
            unsigned integer encoding of idx
        """
        if idx > self.__maxval:
            nbits = (np.log2(self.__maxval + 1)*2)
            self.__uint = np.dtype('uint%d' % nbits).type
            self.__maxval = 2**nbits - 1
            self.__adjust_precision(self.__uint)
        return self.__uint(idx)

    def __adjust_precision(self, uint):
        """
        Adjust precision of data to specificied unsigned integer precision
        """
        if isinstance(self.data, list):
            for i in range(len(self.data)):
                self.data[i] = uint(self.data[i])
        elif isinstance(self.data, np.ndarray):
            self._VectorIndex__data = self.data.astype(uint)
        else:
            raise ValueError("cannot adjust precision of type %s to %s", (type(self.data), uint))

    def add_row(self, arg):
        """
        Convenience function. Same as :py:func:`add_vector`
        """
        self.add_vector(arg)

    def __getitem_helper(self, arg, **kwargs):
        """
        Internal helper function used by __getitem__ to retrieve a data value from self.target

        :param arg: Integer index into this VectorIndex indicating the element we want to retrieve from the target
        :param kwargs: any additional arguments to *get* method of the self.target VectorData
        :return: Scalar or list of values retrieved
        """
        start = 0 if arg == 0 else self.data[arg-1]
        end = self.data[arg]
        return self.target.get(slice(start, end), **kwargs)

    def __getitem__(self, arg):
        """
        Select elements in this VectorIndex and retrieve the corrsponding data from the self.target VectorData

        :param arg: slice or integer index indicating the elements we want to select in this VectorIndex
        :return: Scalar or list of values retrieved
        """
        return self.get(arg)

    def get(self, arg, **kwargs):
        """
        Select elements in this VectorIndex and retrieve the corrsponding data from the self.target VectorData

        :param arg: slice or integer index indicating the elements we want to select in this VectorIndex
        :param kwargs: any additional arguments to *get* method of the self.target VectorData
        :return: Scalar or list of values retrieved
        """
        if np.isscalar(arg):
            return self.__getitem_helper(arg, **kwargs)
        else:
            if isinstance(arg, slice):
                indices = list(range(*arg.indices(len(self.data))))
            else:
                if isinstance(arg[0], bool):
                    arg = np.where(arg)[0]
                indices = arg
            ret = list()
            for i in indices:
                ret.append(self.__getitem_helper(i, **kwargs))
            return ret


@register_class('ElementIdentifiers')
class ElementIdentifiers(Data):
    """
    Data container with a list of unique identifiers for values within a dataset, e.g. rows of a DynamicTable.
    """
    @docval({'name': 'name', 'type': str, 'doc': 'the name of this ElementIdentifiers'},
            {'name': 'data', 'type': ('array_data', 'data'), 'doc': 'a 1D dataset containing identifiers',
             'default': list()})
    def __init__(self, **kwargs):
        call_docval_func(super().__init__, kwargs)

    @docval({'name': 'other', 'type': (Data, np.ndarray, list, tuple, int),
             'doc': 'List of ids to search for in this ElementIdentifer object'},
            rtype=np.ndarray,
            returns='Array with the list of indices where the elements in the list where found.'
                    'Note, the elements in the returned list are ordered in increasing index'
                    'of the found elements, rather than in the order in which the elements'
                    'where given for the search. Also the length of the result may be different from the length'
                    'of the input array. E.g., if our ids are [1,2,3] and we are search for [3,1,5] the '
                    'result would be [0,2] and NOT [2,0,None]')
    def __eq__(self, other):
        """
        Given a list of ids return the indices in the ElementIdentifiers array where the indices are found.
        """
        # Determine the ids we want to find
        search_ids = other if not isinstance(other, Data) else other.data
        if isinstance(search_ids, int):
            search_ids = [search_ids]
        # Find all matching locations
        return np.in1d(self.data, search_ids).nonzero()[0]


@register_class('DynamicTable')
class DynamicTable(Container):
    r"""
    A column-based table. Columns are defined by the argument *columns*. This argument
    must be a list/tuple of :class:`~hdmf.common.table.VectorData` and :class:`~hdmf.common.table.VectorIndex` objects
    or a list/tuple of dicts containing the keys ``name`` and ``description`` that provide the name and description
    of each column in the table. Additionally, the keys ``index``, ``table``, ``vocab`` can be used for specifying
    additional structure to the table columns. Setting the key ``index`` to ``True`` can be used to indicate that the
    :class:`~hdmf.common.table.VectorData` column will store a ragged array (i.e. will be accompanied with a
    :class:`~hdmf.common.table.VectorIndex`). Setting the key ``table`` to ``True`` can be used to indicate that the
    column will store regions to another DynamicTable. Setting the key ``vocab`` to ``True`` can be used to indicate
    that the column data will come from a controlled vocabulary.

    Columns in DynamicTable subclasses can be statically defined by specifying the class attribute *\_\_columns\_\_*,
    rather than specifying them at runtime at the instance level. This is useful for defining a table structure
    that will get reused. The requirements for *\_\_columns\_\_* are the same as the requirements described above
    for specifying table columns with the *columns* argument to the DynamicTable constructor.
    """

    __fields__ = (
        {'name': 'id', 'child': True},
        {'name': 'columns', 'child': True},
        'colnames',
        'description'
    )

    __columns__ = tuple()

    @ExtenderMeta.pre_init
    def __gather_columns(cls, name, bases, classdict):
        r"""
        Gather columns from the *\_\_columns\_\_* class attribute and add them to the class.

        This classmethod will be called during class declaration in the metaclass to automatically
        include all columns declared in subclasses.
        """
        if not isinstance(cls.__columns__, tuple):
            msg = "'__columns__' must be of type tuple, found %s" % type(cls.__columns__)
            raise TypeError(msg)

        if (len(bases) and 'DynamicTable' in globals() and issubclass(bases[-1], Container)
                and bases[-1].__columns__ is not cls.__columns__):
            new_columns = list(cls.__columns__)
            new_columns[0:0] = bases[-1].__columns__  # prepend superclass columns to new_columns
            cls.__columns__ = tuple(new_columns)

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this table'},  # noqa: C901
            {'name': 'description', 'type': str, 'doc': 'a description of what is in this table'},
            {'name': 'id', 'type': ('array_data', 'data', ElementIdentifiers), 'doc': 'the identifiers for this table',
             'default': None},
            {'name': 'columns', 'type': (tuple, list), 'doc': 'the columns in this table', 'default': None},
            {'name': 'colnames', 'type': 'array_data',
             'doc': 'the ordered names of the columns in this table. columns must also be provided.',
             'default': None})
    def __init__(self, **kwargs):  # noqa: C901
        id, columns, desc, colnames = popargs('id', 'columns', 'description', 'colnames', kwargs)
        call_docval_func(super().__init__, kwargs)
        self.description = desc

        # hold names of optional columns that are defined in __columns__ that are not yet initialized
        # map name to column specification
        self.__uninit_cols = dict()

        # All tables must have ElementIdentifiers (i.e. a primary key column)
        # Here, we figure out what to do for that
        if id is not None:
            if not isinstance(id, ElementIdentifiers):
                id = ElementIdentifiers('id', data=id)
        else:
            id = ElementIdentifiers('id')

        if columns is not None and len(columns) > 0:
            # If columns have been passed in, check them over and process accordingly
            if isinstance(columns[0], dict):
                columns = self.__build_columns(columns)
            elif not all(isinstance(c, VectorData) for c in columns):
                raise ValueError("'columns' must be a list of dict, VectorData, DynamicTableRegion, or VectorIndex")

            all_names = [c.name for c in columns]
            if len(all_names) != len(set(all_names)):
                raise ValueError("'columns' contains columns with duplicate names: %s" % all_names)

            all_targets = [c.target.name for c in columns if isinstance(c, VectorIndex)]
            if len(all_targets) != len(set(all_targets)):
                raise ValueError("'columns' contains index columns with the same target: %s" % all_targets)

            # TODO: check columns against __columns__
            # mismatches should raise an error (e.g., a VectorData cannot be passed in with the same name as a
            # prespecified table region column)

            # check column lengths against each other and id length
            # set ids if non-zero cols are provided and ids is empty
            colset = {c.name: c for c in columns}
            for c in columns:  # remove all VectorData objects that have an associated VectorIndex from colset
                if isinstance(c, VectorIndex):
                    if c.target.name in colset:
                        colset.pop(c.target.name)
                    else:
                        raise ValueError("Found VectorIndex '%s' but not its target '%s'" % (c.name, c.target.name))
                _data = c.data
                if isinstance(_data, DataIO):
                    _data = _data.data
                if isinstance(_data, AbstractDataChunkIterator):
                    colset.pop(c.name, None)
            lens = [len(c) for c in colset.values()]
            if not all(i == lens[0] for i in lens):
                raise ValueError("columns must be the same length")
            if len(lens) > 0 and lens[0] != len(id):
                # the first part of this conditional is needed in the
                # event that all columns are AbstractDataChunkIterators
                if len(id) > 0:
                    raise ValueError("must provide same number of ids as length of columns")
                else:  # set ids to: 0 to length of columns - 1
                    id.data.extend(range(lens[0]))

        self.id = id

        # NOTE: self.colnames and self.columns are always tuples
        # if kwarg colnames is an h5dataset, self.colnames is still a tuple
        if colnames is None or len(colnames) == 0:
            if columns is None:
                # make placeholder for columns if nothing was given
                self.colnames = tuple()
                self.columns = tuple()
            else:
                # Figure out column names if columns were given
                tmp = list()
                for col in columns:
                    if isinstance(col, VectorIndex):
                        continue
                    tmp.append(col.name)
                self.colnames = tuple(tmp)
                self.columns = tuple(columns)
        else:
            # Calculate the order of column names
            if columns is None:
                raise ValueError("Must supply 'columns' if specifying 'colnames'")
            else:
                # order the columns according to the column names, which does not include indices
                self.colnames = tuple(pystr(c) for c in colnames)
                col_dict = {col.name: col for col in columns}
                # map from vectordata name to list of vectorindex objects where target of last vectorindex is vectordata
                indices = dict()
                # determine which columns are indexed by another column
                for col in columns:
                    if isinstance(col, VectorIndex):
                        # loop through nested indices to get to non-index column
                        tmp_indices = [col]
                        curr_col = col
                        while isinstance(curr_col.target, VectorIndex):
                            curr_col = curr_col.target
                            tmp_indices.append(curr_col)
                        # make sure the indices values has the full index chain, so replace existing value if it is
                        # shorter
                        if len(tmp_indices) > len(indices.get(curr_col.target.name, [])):
                            indices[curr_col.target.name] = tmp_indices
                    else:
                        if col.name in indices:
                            continue
                        indices[col.name] = []
                # put columns in order of colnames, with indices before the target vectordata
                tmp = []
                for name in self.colnames:
                    tmp.extend(indices[name])
                    tmp.append(col_dict[name])
                self.columns = tuple(tmp)

        # to make generating DataFrames and Series easier
        col_dict = dict()
        self.__indices = dict()
        for col in self.columns:
            if isinstance(col, VectorData) and not isinstance(col, VectorIndex):
                # if we added this column using its index, ignore this column
                if col.name in col_dict:
                    continue
                else:
                    col_dict[col.name] = col
                    self.__set_table_attr(col)
            else:  # col is a vectorindex
                # if index has already been added because it is part of a nested index chain, ignore this column
                if col.name in self.__indices:
                    continue
                self.__indices[col.name] = col

                # loop through nested indices to get to non-index column
                curr_col = col
                self.__set_table_attr(curr_col)
                while isinstance(curr_col.target, VectorIndex):
                    curr_col = curr_col.target
                    # check if index has been added. if not, add it
                    if not hasattr(self, curr_col.name):
                        self.__set_table_attr(curr_col)
                        self.__indices[curr_col.name] = col

                # use target vectordata name at end of indexing chain as key to get to the top level index
                col_dict[curr_col.target.name] = col
                if not hasattr(self, curr_col.target.name):
                    self.__set_table_attr(curr_col.target)

        self.__df_cols = [self.id] + [col_dict[name] for name in self.colnames]

        # self.__colids maps the column name to an index starting at 1
        self.__colids = {name: i+1 for i, name in enumerate(self.colnames)}
        self._init_class_columns()

    def __set_table_attr(self, col):
        if hasattr(self, col.name) and col.name not in self.__uninit_cols:
            msg = ("An attribute '%s' already exists on %s '%s' so this column cannot be accessed as an attribute, "
                   "e.g., table.%s; it can only be accessed using other methods, e.g., table['%s']."
                   % (col.name, self.__class__.__name__, self.name, col.name, col.name))
            warn(msg)
        else:
            setattr(self, col.name, col)

    def _init_class_columns(self):
        """
        Process all predefined columns specified in class variable __columns__.
        Optional columns are not tracked but not added.
        """
        for col in self.__columns__:
            if col['name'] not in self.__colids:  # if column has not been added in __init__
                if col.get('required', False):
                    self.add_column(name=col['name'],
                                    description=col['description'],
                                    index=col.get('index', False),
                                    table=col.get('table', False),
                                    # Pass through extra kwargs for add_column that subclasses may have added
                                    **{k: col[k] for k in col.keys()
                                        if k not in ['name', 'description', 'index', 'table', 'required']})
                else:
                    # track the not yet initialized optional predefined columns
                    self.__uninit_cols[col['name']] = col

                    # set the table attributes for not yet init optional predefined columns
                    setattr(self, col['name'], None)
                    if col.get('index', False):
                        self.__uninit_cols[col['name'] + '_index'] = col
                        setattr(self, col['name'] + '_index', None)

    @staticmethod
    def __build_columns(columns, df=None):
        """
        Build column objects according to specifications
        """
        tmp = list()
        for d in columns:
            name = d['name']
            desc = d.get('description', 'no description')
            data = None
            if df is not None:
                data = list(df[name].values)
            if d.get('index', False):
                index_data = None
                if data is not None:
                    index_data = [len(data[0])]
                    for i in range(1, len(data)):
                        index_data.append(len(data[i]) + index_data[i-1])
                    # assume data came in through a DataFrame, so we need
                    # to concatenate it
                    tmp_data = list()
                    for d in data:
                        tmp_data.extend(d)
                    data = tmp_data
                vdata = VectorData(name, desc, data=data)
                vindex = VectorIndex("%s_index" % name, index_data, target=vdata)
                tmp.append(vindex)
                tmp.append(vdata)
            else:
                if data is None:
                    data = list()
                cls = VectorData
                if d.get('table', False):
                    cls = DynamicTableRegion
                tmp.append(cls(name, desc, data=data))
        return tmp

    def __len__(self):
        """Number of rows in the table"""
        return len(self.id)

    @docval({'name': 'data', 'type': dict, 'doc': 'the data to put in this row', 'default': None},
            {'name': 'id', 'type': int, 'doc': 'the ID for the row', 'default': None},
            {'name': 'enforce_unique_id', 'type': bool, 'doc': 'enforce that the id in the table must be unique',
             'default': False},
            allow_extra=True)
    def add_row(self, **kwargs):
        """
        Add a row to the table. If *id* is not provided, it will auto-increment.
        """
        data, row_id, enforce_unique_id = popargs('data', 'id', 'enforce_unique_id', kwargs)
        data = data if data is not None else kwargs

        extra_columns = set(list(data.keys())) - set(list(self.__colids.keys()))
        missing_columns = set(list(self.__colids.keys())) - set(list(data.keys()))

        # check to see if any of the extra columns just need to be added
        if extra_columns:
            for col in self.__columns__:
                if col['name'] in extra_columns:
                    if data[col['name']] is not None:
                        self.add_column(col['name'], col['description'],
                                        index=col.get('index', False),
                                        table=col.get('table', False),
                                        # Pass through extra keyword arguments for add_column that
                                        # subclasses may have added
                                        **{k: col[k] for k in col.keys()
                                            if k not in ['name', 'description', 'index', 'table', 'required']})
                    extra_columns.remove(col['name'])

        if extra_columns or missing_columns:
            raise ValueError(
                '\n'.join([
                    'row data keys don\'t match available columns',
                    'you supplied {} extra keys: {}'.format(len(extra_columns), extra_columns),
                    'and were missing {} keys: {}'.format(len(missing_columns), missing_columns)
                ])
            )
        if row_id is None:
            row_id = data.pop('id', None)
        if row_id is None:
            row_id = len(self)
        if enforce_unique_id:
            if row_id in self.id:
                raise ValueError("id %i already in the table" % row_id)
        self.id.append(row_id)

        for colname, colnum in self.__colids.items():
            if colname not in data:
                raise ValueError("column '%s' missing" % colname)
            c = self.__df_cols[colnum]
            if isinstance(c, VectorIndex):
                c.add_vector(data[colname])
            else:
                c.add_row(data[colname])

    def __eq__(self, other):
        """
        Compare if the two DynamicTables contain the same data

        This implemented by converting the DynamicTables to a pandas dataframe and
        comparing the equality of the two tables.

        :param other: DynamicTable to compare to

        :raises: An error will be raised with to_dataframe is not defined or other

        :return: Bool indicating whether the two DynamicTables contain the same data
        """
        return self.to_dataframe().equals(other.to_dataframe())

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this VectorData'},  # noqa: C901
            {'name': 'description', 'type': str, 'doc': 'a description for this column'},
            {'name': 'data', 'type': ('array_data', 'data'),
             'doc': 'a dataset where the first dimension is a concatenation of multiple vectors', 'default': list()},
            {'name': 'table', 'type': (bool, 'DynamicTable'),
             'doc': 'whether or not this is a table region or the table the region applies to', 'default': False},
            {'name': 'index', 'type': (bool, VectorIndex, 'array_data'),
             'doc': 'whether or not this column should be indexed', 'default': False},
            {'name': 'vocab', 'type': (bool, 'array_data'), 'default': False,
             'doc': ('whether or not this column contains data from a '
                     'controlled vocabulary or the controlled vocabulary')})
    def add_column(self, **kwargs):  # noqa: C901
        """
        Add a column to this table.

        If data is provided, it must contain the same number of rows as the current state of the table.

        :raises ValueError: if the column has already been added to the table
        """
        name, data = getargs('name', 'data', kwargs)
        index, table, vocab = popargs('index', 'table', 'vocab', kwargs)

        if isinstance(index, VectorIndex):
            warn("Passing a VectorIndex in for index may lead to unexpected behavior. This functionality will be "
                 "deprecated in a future version of HDMF.", FutureWarning)

        if name in self.__colids:  # column has already been added
            msg = "column '%s' already exists in %s '%s'" % (name, self.__class__.__name__, self.name)
            raise ValueError(msg)

        if name in self.__uninit_cols:  # column is a predefined optional column from the spec
            # check the given values against the predefined optional column spec. if they do not match, raise a warning
            # and ignore the given arguments. users should not be able to override these values
            table_bool = table or not isinstance(table, bool)
            spec_table = self.__uninit_cols[name].get('table', False)
            if table_bool != spec_table:
                msg = ("Column '%s' is predefined in %s with table=%s which does not match the entered "
                       "table argument. The predefined table spec will be ignored. "
                       "Please ensure the new column complies with the spec. "
                       "This will raise an error in a future version of HDMF."
                       % (name, self.__class__.__name__, spec_table))
                warn(msg)

            index_bool = index or not isinstance(index, bool)
            spec_index = self.__uninit_cols[name].get('index', False)
            if index_bool != spec_index:
                msg = ("Column '%s' is predefined in %s with index=%s which does not match the entered "
                       "index argument. The predefined index spec will be ignored. "
                       "Please ensure the new column complies with the spec. "
                       "This will raise an error in a future version of HDMF."
                       % (name, self.__class__.__name__, spec_index))
                warn(msg)

        ckwargs = dict(kwargs)
        cls = VectorData

        # Add table if it's been specified
        if table and vocab:
            raise ValueError("column '%s' cannot be both a table region and come from a controlled vocabulary" % name)
        if table is not False:
            cls = DynamicTableRegion
            if isinstance(table, DynamicTable):
                ckwargs['table'] = table
        if vocab is not False:
            cls = VocabData
            if isinstance(vocab, (list, tuple, np.ndarray)):
                ckwargs['vocabulary'] = vocab

        col = cls(**ckwargs)
        col.parent = self
        columns = [col]
        self.__set_table_attr(col)
        if col in self.__uninit_cols:
            self.__uninit_cols.pop(col)

        # Add index if it's been specified
        if index is not False:
            if isinstance(index, VectorIndex):
                col_index = index
            elif isinstance(index, bool):        # make empty VectorIndex
                if len(col) > 0:
                    raise ValueError("cannot pass empty index with non-empty data to index")
                col_index = VectorIndex(name + "_index", list(), col)
            else:                                # make VectorIndex with supplied data
                if len(col) == 0:
                    raise ValueError("cannot pass non-empty index with empty data to index")
                col_index = VectorIndex(name + "_index", index, col)
            columns.insert(0, col_index)
            if not isinstance(col_index.parent, Container):
                col_index.parent = self
            # else, the ObjectMapper will create a link from self (parent) to col_index (child with existing parent)
            col = col_index
            self.__indices[col_index.name] = col_index
            self.__set_table_attr(col_index)
            if col_index in self.__uninit_cols:
                self.__uninit_cols.pop(col_index)

        if len(col) != len(self.id):
            raise ValueError("column must have the same number of rows as 'id'")
        self.__colids[name] = len(self.__df_cols)
        self.fields['colnames'] = tuple(list(self.colnames)+[name])
        self.fields['columns'] = tuple(list(self.columns)+columns)
        self.__df_cols.append(col)

    @docval({'name': 'name', 'type': str, 'doc': 'the name of the DynamicTableRegion object'},
            {'name': 'region', 'type': (slice, list, tuple), 'doc': 'the indices of the table'},
            {'name': 'description', 'type': str, 'doc': 'a brief description of what the region is'})
    def create_region(self, **kwargs):
        """
        Create a DynamicTableRegion selecting a region (i.e., rows) in this DynamicTable.

        :raises: IndexError if the provided region contains invalid indices

        """
        region = getargs('region', kwargs)
        if isinstance(region, slice):
            if (region.start is not None and region.start < 0) or (region.stop is not None and region.stop > len(self)):
                msg = 'region slice %s is out of range for this DynamicTable of length %d' % (str(region), len(self))
                raise IndexError(msg)
            region = list(range(*region.indices(len(self))))
        else:
            for idx in region:
                if idx < 0 or idx >= len(self):
                    raise IndexError('The index ' + str(idx) +
                                     ' is out of range for this DynamicTable of length '
                                     + str(len(self)))
        desc = getargs('description', kwargs)
        name = getargs('name', kwargs)
        return DynamicTableRegion(name, region, desc, self)

    def __getitem__(self, key):
        ret = self.get(key)
        if ret is None:
            raise KeyError(key)
        return self.get(key)

    def get(self, key, default=None, df=True, **kwargs):  # noqa: C901
        """
        Select a subset from the table

        :param key: Key defining which elements of the table to select. This may be one of the following:

            1) string with the name of the column to select
            2) a tuple consisting of (str, int) where the string identifies the column to select by name
               and the int selects the row
            3) int, list of ints, or slice selecting a set of full rows in the table

        :return: 1) If key is a string, then return array with the data of the selected column
                 2) If key is a tuple of (int, str), then return the scalar value of the selected cell
                 3) If key is an int, list or slice, then return pandas.DataFrame consisting of one or more rows

        :raises: KeyError
        """
        ret = None
        if isinstance(key, tuple):
            # index by row and column --> return specific cell
            arg1 = key[0]
            arg2 = key[1]
            if isinstance(arg2, str):
                arg2 = self.__colids[arg2]
            ret = self.__df_cols[arg2][arg1]
        elif isinstance(key, str):
            # index by one string --> return column
            if key == 'id':
                return self.id
            elif key in self.__colids:
                ret = self.__df_cols[self.__colids[key]]
            elif key in self.__indices:
                ret = self.__indices[key]
            else:
                return default
        else:
            # index by int, list, or slice --> return pandas Dataframe consisting of one or more rows
            # determine the key. If the key is an int, then turn it into a slice to reduce the number of cases below
            arg = key
            try:
                if np.issubdtype(type(arg), np.integer):
                    ret = OrderedDict()
                    ret['id'] = self.id.data[arg]
                    for name in self.colnames:
                        col = self.__df_cols[self.__colids[name]]
                        ret[name] = col.get(arg, df=df, **kwargs)
                # index with a python slice (or single integer) to select one or multiple rows
                elif isinstance(arg, slice):
                    ret = OrderedDict()
                    ret['id'] = self.id.data[arg]
                    for name in self.colnames:
                        col = self.__df_cols[self.__colids[name]]
                        if isinstance(col.data, (Dataset, np.ndarray)) and col.data.ndim > 1:
                            ret[name] = col.get(arg, df=df, **kwargs)
                        else:
                            currdata = col.get(arg, df=df, **kwargs)
                            ret[name] = currdata
                # index by a list of ints, return multiple rows
                elif isinstance(arg, (tuple, list, np.ndarray)):
                    if isinstance(arg, np.ndarray):
                        if len(arg.shape) != 1:
                            raise ValueError("cannot index DynamicTable with multiple dimensions")
                    ret = OrderedDict()
                    ret['id'] = (self.id.data[arg]
                                 if isinstance(self.id.data, np.ndarray)
                                 else [self.id.data[i] for i in arg])
                    for name in self.colnames:
                        col = self.__df_cols[self.__colids[name]]
                        if isinstance(col.data, (Dataset, np.ndarray)) and col.data.ndim > 1:
                            ret[name] = [x for x in col.get(arg, df=df, **kwargs)]
                        elif isinstance(col.data, (list, np.ndarray)):
                            ret[name] = col.get(arg, df=df, **kwargs)
                        else:
                            ret[name] = [col.get(arg, df=df, **kwargs) for i in arg]
                else:
                    raise KeyError("Key type not supported by DynamicTable %s" % str(type(arg)))
            except ValueError as ve:
                x = re.match(r"^Index \((.*)\) out of range \(.*\)$", str(ve))
                if x:
                    msg = ("Row index %s out of range for %s '%s' (length %d)."
                           % (x.groups()[0], self.__class__.__name__, self.name, len(self)))
                    raise IndexError(msg)
                else:  # pragma: no cover
                    raise ve
            except IndexError as ie:
                if str(ie) == 'list index out of range':
                    msg = ("Row index out of range for %s '%s' (length %d)."
                           % (self.__class__.__name__, self.name, len(self)))
                    raise IndexError(msg)
                else:  # pragma: no cover
                    raise ie

            if df:
                # reformat objects to fit into a pandas DataFrame
                id_index = ret.pop('id')
                if np.isscalar(id_index):
                    id_index = [id_index]
                retdf = OrderedDict()
                for k in ret:
                    if isinstance(ret[k], np.ndarray):
                        if ret[k].ndim == 1:
                            if len(id_index) == 1:
                                # k is a multi-dimension column, and
                                # only one element has been selected
                                retdf[k] = [ret[k]]
                            else:
                                retdf[k] = ret[k]
                        else:
                            if len(id_index) == ret[k].shape[0]:
                                # k is a multi-dimension column, and
                                # more than one element has been selected
                                retdf[k] = list(ret[k])
                            else:
                                raise ValueError('unable to convert selection to DataFrame')
                    elif isinstance(ret[k], list):
                        if len(id_index) == 1:
                            # k is a multi-dimension column, and
                            # only one element has been selected
                            retdf[k] = [ret[k]]
                        else:
                            retdf[k] = ret[k]
                    elif isinstance(ret[k], pd.DataFrame):
                        retdf['%s_%s' % (k, ret[k].index.name)] = ret[k].index.values
                        for col in ret[k].columns:
                            newcolname = "%s_%s" % (k, col)
                            retdf[newcolname] = ret[k][col].values
                    else:
                        retdf[k] = ret[k]

                ret = pd.DataFrame(retdf, index=pd.Index(name=self.id.name, data=id_index))
            else:
                ret = list(ret.values())

        return ret

    def __contains__(self, val):
        """
        Check if the given value (i.e., column) exists in this table
        """
        return val in self.__colids or val in self.__indices

    @docval({'name': 'exclude', 'type': set, 'doc': ' Set of columns to exclude from the dataframe', 'default': None})
    def to_dataframe(self, **kwargs):
        """
        Produce a pandas DataFrame containing this table's data.
        """
        exclude = popargs('exclude', kwargs)
        if exclude is None:
            exclude = set([])
        data = OrderedDict()
        for name in self.colnames:
            if name in exclude:
                continue
            col = self.__df_cols[self.__colids[name]]

            if isinstance(col.data, (Dataset, np.ndarray)) and col.data.ndim > 1:
                data[name] = [x for x in col[:]]
            else:
                data[name] = col[:]

        return pd.DataFrame(data, index=pd.Index(name=self.id.name, data=self.id.data))

    @classmethod
    @docval(
        {'name': 'df', 'type': pd.DataFrame, 'doc': 'source DataFrame'},
        {'name': 'name', 'type': str, 'doc': 'the name of this table'},
        {
            'name': 'index_column',
            'type': str,
            'doc': 'if provided, this column will become the table\'s index',
            'default': None
        },
        {
            'name': 'table_description',
            'type': str,
            'doc': 'a description of what is in the resulting table',
            'default': ''
        },
        {
            'name': 'columns',
            'type': (list, tuple),
            'doc': 'a list/tuple of dictionaries specifying columns in the table',
            'default': None
        },
        allow_extra=True
    )
    def from_dataframe(cls, **kwargs):
        '''
        Construct an instance of DynamicTable (or a subclass) from a pandas DataFrame.

        The columns of the resulting table are defined by the columns of the
        dataframe and the index by the dataframe's index (make sure it has a
        name!) or by a column whose name is supplied to the index_column
        parameter. We recommend that you supply *columns* - a list/tuple of
        dictionaries containing the name and description of the column- to help
        others understand the contents of your table. See
        :py:class:`~hdmf.common.table.DynamicTable` for more details on *columns*.
        '''

        columns = kwargs.pop('columns')
        df = kwargs.pop('df')
        name = kwargs.pop('name')
        index_column = kwargs.pop('index_column')
        table_description = kwargs.pop('table_description')
        column_descriptions = kwargs.pop('column_descriptions', dict())

        supplied_columns = dict()
        if columns:
            supplied_columns = {x['name']: x for x in columns}

        class_cols = {x['name']: x for x in cls.__columns__}
        required_cols = set(x['name'] for x in cls.__columns__ if 'required' in x and x['required'])
        df_cols = df.columns
        if required_cols - set(df_cols):
            raise ValueError('missing required cols: ' + str(required_cols - set(df_cols)))
        if set(supplied_columns.keys()) - set(df_cols):
            raise ValueError('cols specified but not provided: ' + str(set(supplied_columns.keys()) - set(df_cols)))
        columns = []
        for col_name in df_cols:
            if col_name in class_cols:
                columns.append(class_cols[col_name])
            elif col_name in supplied_columns:
                columns.append(supplied_columns[col_name])
            else:
                columns.append({'name': col_name,
                                'description': column_descriptions.get(col_name, 'no description')})
                if hasattr(df[col_name].iloc[0], '__len__') and not isinstance(df[col_name].iloc[0], str):
                    lengths = [len(x) for x in df[col_name]]
                    if not lengths[1:] == lengths[:-1]:
                        columns[-1].update(index=True)

        if index_column is not None:
            ids = ElementIdentifiers(name=index_column, data=df[index_column].values.tolist())
        else:
            index_name = df.index.name if df.index.name is not None else 'id'
            ids = ElementIdentifiers(name=index_name, data=df.index.values.tolist())

        columns = cls.__build_columns(columns, df=df)

        return cls(name=name, id=ids, columns=columns, description=table_description, **kwargs)

    def copy(self):
        """
        Return a copy of this DynamicTable.
        This is useful for linking.
        """
        kwargs = dict(name=self.name, id=self.id, columns=self.columns, description=self.description,
                      colnames=self.colnames)
        return self.__class__(**kwargs)


@register_class('DynamicTableRegion')
class DynamicTableRegion(VectorData):
    """
    DynamicTableRegion provides a link from one table to an index or region of another. The `table`
    attribute is another `DynamicTable`, indicating which table is referenced. The data is int(s)
    indicating the row(s) (0-indexed) of the target array. `DynamicTableRegion`s can be used to
    associate multiple rows with the same meta-data without data duplication. They can also be used to
    create hierarchical relationships between multiple `DynamicTable`s. `DynamicTableRegion` objects
    may be paired with a `VectorIndex` object to create ragged references, so a single cell of a
    `DynamicTable` can reference many rows of another `DynamicTable`.
    """

    __fields__ = (
        'table',
        'description'
    )

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this VectorData'},
            {'name': 'data', 'type': ('array_data', 'data'),
             'doc': 'a dataset where the first dimension is a concatenation of multiple vectors'},
            {'name': 'description', 'type': str, 'doc': 'a description of what this region represents'},
            {'name': 'table', 'type': DynamicTable,
             'doc': 'the DynamicTable this region applies to', 'default': None})
    def __init__(self, **kwargs):
        t = popargs('table', kwargs)
        call_docval_func(super().__init__, kwargs)
        self.table = t

    @property
    def table(self):
        """The DynamicTable this DynamicTableRegion is pointing to"""
        return self.fields.get('table')

    @table.setter
    def table(self, val):
        """
        Set the table this DynamicTableRegion should be pointing to

        :param val: The DynamicTable this DynamicTableRegion should be pointing to

        :raises: AttributeError if table is already in fields
        :raises: IndexError if the current indices are out of bounds for the new table given by val
        """
        if val is None:
            return
        if 'table' in self.fields:
            msg = "can't set attribute 'table' -- already set"
            raise AttributeError(msg)
        dat = self.data
        if isinstance(dat, DataIO):
            dat = dat.data
        self.fields['table'] = val

    def __getitem__(self, arg):
        return self.get(arg)

    def get(self, arg, index=False, **kwargs):
        """
        Subset the DynamicTableRegion

        :param arg: 1) tuple consisting of (str, int) where the string defines the column to select
                       and the int selects the row, 2) int or slice to select a subset of rows

        :return: Result from self.table[....] with the appropritate selection based on the
                 rows selected by this DynamicTableRegion
        """
        # treat the list of indices as data that can be indexed. then pass the
        # result to the table to get the data
        if isinstance(arg, tuple):
            arg1 = arg[0]
            arg2 = arg[1]
            return self.table[self.data[arg1], arg2]
        elif isinstance(arg, slice) or np.issubdtype(type(arg), np.integer):
            if np.issubdtype(type(arg), np.integer) and arg >= len(self.data):
                raise IndexError('index {} out of bounds for data of length {}'.format(arg, len(self.data)))
            ret = self.data[arg]
            if not index:
                ret = self.table.get(ret, **kwargs)
            return ret
        else:
            raise ValueError("unrecognized argument: '%s'" % arg)

    def to_dataframe(self, **kwargs):
        """
        Convert the whole DynamicTableRegion to a pandas dataframe.

        Keyword arguments are passed through to the to_dataframe method of DynamicTable that
        is being referenced (i.e., self.table). This allows specification of the 'exclude'
        parameter and any other parameters of DynamicTable.to_dataframe.
        """
        return self.table.to_dataframe(**kwargs).iloc[self.data[:]]

    @property
    def shape(self):
        """
        Define the shape, i.e., (num_rows, num_columns) of the selected table region
        :return: Shape tuple with two integers indicating the number of rows and number of columns
        """
        return (len(self.data), len(self.table.columns))

    def __repr__(self):
        """
        :return: Human-readable string representation of the DynamicTableRegion
        """
        cls = self.__class__
        template = "%s %s.%s at 0x%d\n" % (self.name, cls.__module__, cls.__name__, id(self))
        template += "    Target table: %s %s.%s at 0x%d\n" % (self.table.name,
                                                              self.table.__class__.__module__,
                                                              self.table.__class__.__name__,
                                                              id(self.table))
        return template


@register_class('VocabData')
class VocabData(VectorData):
    """
    A n-dimensional dataset that can contain elements from a controlled
    vocabulary.
    """

    __fields__ = ('vocabulary',)

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this VectorData'},
            {'name': 'description', 'type': str, 'doc': 'a description for this column'},
            {'name': 'data', 'type': ('array_data', 'data'),
             'doc': 'a dataset where the first dimension is a concatenation of multiple vectors', 'default': list()},
            {'name': 'vocabulary', 'type': ('array_data', 'data'), 'default': list(),
             'doc': 'the items in this vocabulary'})
    def __init__(self, **kwargs):
        vocab = popargs('vocabulary', kwargs)
        super().__init__(**kwargs)
        if len(vocab) > 0:
            self.vocabulary = np.asarray(vocab)
            self.__uint = self.__uint_precision(vocab)
            self.__revidx = self.__map_vocab(self.__uint, self.vocabulary)
        else:
            self.vocabulary = vocab
            self.__revidx = dict()  # a map from term to index
            self.__uint = None      # the precision needed to encode all terms

    @staticmethod
    def __uint_precision(vocab):
        """ Calculate the uint precision needed to encode the given vocabulary """
        return np.dtype('uint%d' % 8 * max(1, int((2 ** np.ceil((np.ceil(np.log2(len(vocab))) - 8)/8))))).type

    @staticmethod
    def __map_vocab(uint, vocab):
        """ Map CV terms to their uint index """
        return {t[1]: uint(t[0]) for t in enumerate(vocab)}

    def __add_term(self, term):
        """
        Add a new CV term, and return it's corresponding index

        Returns:
            The index of the term
        """
        if term not in self.__revidx:
            # get minimum uint precision needed for vocabulary
            self.vocabulary.append(term)
            uint = self.__uint_precision(self.vocabulary)
            if self.__uint is uint:
                # add the new term to the index-term map
                self.__revidx[term] = self.__uint(len(self.vocabulary)-1)
            else:
                # remap terms to their uint and bump the precision of existing data
                self.__uint = uint
                self.__revidx = self.__map_vocab(self.__uint, self.vocabulary)
                for i in range(len(self.data)):
                    self.data[i] = self.__uint(self.data[i])
        return self.__revidx[term]

    def __getitem__(self, arg):
        return self.get(arg, index=False)

    def _get_helper(self, idx, index=False, join=False, **kwargs):
        """
        A helper function for getting vocabulary elements

        This helper function contains the post-processing of retrieve indices. By separating this,
        it allows customizing processing of indices before resolving the vocabulary elements
        """
        if index:
            return idx
        if not np.isscalar(idx):
            orig_shape = idx.shape
            ret = self.vocabulary[idx.ravel()]
            ret = ret.reshape(orig_shape)
            if join:
                ret = ''.join(ret.ravel())
        else:
            ret = self.vocabulary[idx]
        return ret

    def get(self, arg, index=False, join=False, **kwargs):
        """
        Return vocabulary elements for the given argument.

        Args:
            index (bool):      Return indices, do not return CV elements
            join (bool):       Concatenate elements together into a single string

        Returns:
            CV elements if *join* is False or a concatenation of all selected
            elements if *join* is True.
        """
        idx = self.data[arg]
        return self._get_helper(idx, index=index, join=join, **kwargs)

    @docval({'name': 'val', 'type': None, 'doc': 'the value to add to this column'},
            {'name': 'index', 'type': bool, 'doc': 'whether or not the value being added is an index',
             'default': False})
    def add_row(self, **kwargs):
        """Append a data value to this VocabData column

        If a controlled-vocabulary is provided for *val* (i.e. *index* is False), the correct
        index value will be determined. Otherwise, *val* will be added as provided.
        """
        val, index = getargs('val', 'index', kwargs)
        if not index:
            val = self.__add_term(val)
        super().append(val)
