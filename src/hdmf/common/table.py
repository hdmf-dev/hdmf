"""
Collection of Container classes for interacting with data types related to
the storage and use of dynamic data tables as part of the hdmf-common schema
"""

from h5py import Dataset
import numpy as np
import pandas as pd
from collections import OrderedDict
from warnings import warn

from ..utils import docval, getargs, ExtenderMeta, call_docval_func, popargs, pystr
from ..container import Container, Data

from . import register_class


@register_class('Index')
class Index(Data):
    """
    Base data type for storing pointers that index data values
    """
    __fields__ = ("target",)

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this VectorData'},
            {'name': 'data', 'type': ('array_data', 'data'),
             'doc': 'a dataset where the first dimension is a concatenation of multiple vectors'},
            {'name': 'target', 'type': Data,
             'doc': 'the target dataset that this index applies to'})
    def __init__(self, **kwargs):
        call_docval_func(super().__init__, kwargs)


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


@register_class('VectorIndex')
class VectorIndex(Index):
    """
    When paired with a VectorData, this allows for storing arrays of varying
    length in a single cell of the DynamicTable by indexing into this VectorData.
    The first vector is at VectorData[0:VectorIndex(0)+1]. The second vector is at
    VectorData[VectorIndex(0)+1:VectorIndex(1)+1], and so on.
    """

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this VectorIndex'},
            {'name': 'data', 'type': ('array_data', 'data'),
             'doc': 'a 1D dataset containing indexes that apply to VectorData object'},
            {'name': 'target', 'type': VectorData,
             'doc': 'the target dataset that this index applies to'})
    def __init__(self, **kwargs):
        call_docval_func(super().__init__, kwargs)
        self.target = getargs('target', kwargs)

    def add_vector(self, arg):
        """
        Add the given data value to the target VectorData and append the corresponding index to this VectorIndex
        :param arg: The data value to be added to self.target
        """
        self.target.extend(arg)
        self.append(len(self.target))

    def add_row(self, arg):
        """
        Convenience function. Same as :py:func:`add_vector`
        """
        self.add_vector(arg)

    def __getitem_helper(self, arg):
        """
        Internal helper function used by __getitem__ to retrieve a data value from self.target

        :param arg: Integer index into this VectorIndex indicating the element we want to retrieve from the target
        """
        start = 0 if arg == 0 else self.data[arg-1]
        end = self.data[arg]
        return self.target[start:end]

    def __getitem__(self, arg):
        """
        Select elements in this VectorIndex and retrieve the corrsponding data from the self.target VectorData

        :param arg: slice or integer index indicating the elements we want to select in this VectorIndex
        :return: Scalar or list of values retrieved
        """
        if isinstance(arg, slice):
            indices = list(range(*arg.indices(len(self.data))))
            ret = list()
            for i in indices:
                ret.append(self.__getitem_helper(i))
            return ret
        else:
            return self.__getitem_helper(arg)


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
    of each column in the table. Additionally, the keys ``index`` and ``table`` for specifying additional structure to
    the table columns. Setting the key ``index`` to ``True`` can be used to indicate that the
    :class:`~hdmf.common.table.VectorData` column will store a ragged array (i.e. will be accompanied with a
    :class:`~hdmf.common.table.VectorIndex`). Setting the key ``table`` to ``True`` can be used to indicate that the
    column will store regions to another DynamicTable.

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
            new_columns[0:0] = bases[-1].__columns__
            cls.__columns__ = tuple(new_columns)

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this table'},
            {'name': 'description', 'type': str, 'doc': 'a description of what is in this table'},
            {'name': 'id', 'type': ('array_data', ElementIdentifiers), 'doc': 'the identifiers for this table',
             'default': None},
            {'name': 'columns', 'type': (tuple, list), 'doc': 'the columns in this table', 'default': None},
            {'name': 'colnames', 'type': 'array_data', 'doc': 'the names of the columns in this table',
             'default': None})
    def __init__(self, **kwargs):  # noqa: C901
        id, columns, desc, colnames = popargs('id', 'columns', 'description', 'colnames', kwargs)
        call_docval_func(super().__init__, kwargs)
        self.description = desc

        # All tables must have ElementIdentifiers (i.e. a primary key column)
        # Here, we figure out what to do for that
        if id is not None:
            if not isinstance(id, ElementIdentifiers):
                id = ElementIdentifiers('id', data=id)
        else:
            id = ElementIdentifiers('id')

        if columns is not None:
            if len(columns) > 0:
                # If columns have been passed in, check them over
                # and process accordingly
                if isinstance(columns[0], dict):
                    columns = self.__build_columns(columns)
                elif not all(isinstance(c, (VectorData, VectorIndex)) for c in columns):
                    raise ValueError("'columns' must be a list of VectorData, DynamicTableRegion or VectorIndex")
                colset = {c.name: c for c in columns}
                for c in columns:
                    if isinstance(c, VectorIndex):
                        colset.pop(c.target.name)
                lens = [len(c) for c in colset.values()]
                if not all(i == lens[0] for i in lens):
                    raise ValueError("columns must be the same length")
                if lens[0] != len(id):
                    if len(id) > 0:
                        raise ValueError("must provide same number of ids as length of columns")
                    else:
                        id.data.extend(range(lens[0]))
        else:
            # if the user has not passed in columns, make a place to put them,
            # as they will presumably be adding new columns
            columns = list()

        self.id = id

        if colnames is None:
            if columns is None:
                # make placeholder for columns if nothing was given
                self.colnames = list()
                self.columns = list()
            else:
                # Figure out column names if columns were given
                tmp = list()
                for col in columns:
                    if isinstance(col, VectorIndex):
                        continue
                    tmp.append(col.name)
                self.colnames = tuple(tmp)
                self.columns = columns
        else:
            # Calculate the order of column names
            if columns is None:
                raise ValueError("Must supply 'columns' if specifying 'colnames'")
            else:
                # order the columns according to the column names
                self.colnames = tuple(pystr(c) for c in colnames)
                col_dict = {col.name: col for col in columns}
                order = dict()
                indexed = dict()
                for col in columns:
                    if isinstance(col, VectorIndex):
                        indexed[col.target.name] = True
                    else:
                        if col.name in indexed:
                            continue
                        indexed[col.name] = False
                i = 0
                for name in self.colnames:
                    col = col_dict[name]
                    order[col.name] = i
                    if indexed[col.name]:
                        i = i + 1
                    i = i + 1
                tmp = [None] * i
                for col in columns:
                    if indexed.get(col.name, False):
                        continue
                    if isinstance(col, VectorData):
                        pos = order[col.name]
                        tmp[pos] = col
                    elif isinstance(col, VectorIndex):
                        pos = order[col.target.name]
                        tmp[pos] = col
                        tmp[pos+1] = col.target
                self.columns = list(tmp)

        # to make generating DataFrames and Series easier
        col_dict = dict()
        self.__indices = dict()
        for col in self.columns:
            self.__set_table_attr(col)
            if isinstance(col, VectorData):
                existing = col_dict.get(col.name)
                # if we added this column using its index, ignore this column
                if existing is not None:
                    if isinstance(existing, VectorIndex):
                        if existing.target.name == col.name:
                            continue
                        else:
                            raise ValueError("duplicate column does not target VectorData '%s'" % col.name)
                    else:
                        raise ValueError("duplicate column found: '%s'" % col.name)
                else:
                    col_dict[col.name] = col
            elif isinstance(col, VectorIndex):
                col_dict[col.target.name] = col  # use target name for reference and VectorIndex for retrieval
                self.__indices[col.name] = col

        self.__df_cols = [self.id] + [col_dict[name] for name in self.colnames]
        self.__colids = {name: i+1 for i, name in enumerate(self.colnames)}
        self._init_class_columns()

    def __set_table_attr(self, col):
        if hasattr(self, col.name):
            msg = ("An attribute '%s' already exists on %s '%s' so this column cannot be accessed as an attribute, "
                   "e.g., table.%s; it can only be accessed using other methods, e.g., table['%s']."
                   % (col.name, self.__class__.__name__, self.name, col.name, col.name))
            warn(msg)
        else:
            setattr(self, col.name, col)

    def _init_class_columns(self):
        self.__uninit_cols = []  # hold column names that are defined in __columns__ but not yet initialized
        for col in self.__columns__:
            if col['name'] not in self.__colids:
                if col.get('required', False):
                    self._add_column(col['name'], col['description'],
                                     index=col.get('index', False),
                                     table=col.get('table', False),
                                     # Pass through extra keyword arguments for _add_column that subclasses may have
                                     # added
                                     **{k: col[k] for k in col.keys()
                                        if k not in ['name', 'description', 'index', 'table', 'required']})

                else:  # create column name attributes (set to None) on the object even if column is not required
                    self.__uninit_cols.append(col['name'])
                    setattr(self, col['name'], None)
                    if col.get('index', False):
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
                        self._add_column(col['name'], col['description'],
                                         index=col.get('index', False),
                                         table=col.get('table', False),
                                         # Pass through extra keyword arguments for _add_column that
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

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this VectorData'},
            {'name': 'description', 'type': str, 'doc': 'a description for this column'},
            {'name': 'data', 'type': ('array_data', 'data'),
             'doc': 'a dataset where the first dimension is a concatenation of multiple vectors', 'default': list()},
            {'name': 'table', 'type': (bool, 'DynamicTable'),
             'doc': 'whether or not this is a table region or the table the region applies to', 'default': False},
            {'name': 'index', 'type': (bool, VectorIndex, 'array_data'),
             'doc': 'whether or not this column should be indexed', 'default': False})
    def add_column(self, **kwargs):
        name = getargs('name', kwargs)
        for col in self.__columns__:
            if col['name'] == name:  # column has not been added but is pre-specified
                msg = "column '%s' already exists in %s '%s'" % (name, self.__class__.__name__, self.name)
                raise ValueError(msg)

        self._add_column(**kwargs)

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this VectorData'},
            {'name': 'description', 'type': str, 'doc': 'a description for this column'},
            {'name': 'data', 'type': ('array_data', 'data'),
             'doc': 'a dataset where the first dimension is a concatenation of multiple vectors', 'default': list()},
            {'name': 'table', 'type': (bool, 'DynamicTable'),
             'doc': 'whether or not this is a table region or the table the region applies to', 'default': False},
            {'name': 'index', 'type': (bool, VectorIndex, 'array_data'),
             'doc': 'whether or not this column should be indexed', 'default': False})
    def _add_column(self, **kwargs):
        """
        Add a column to this table.

        If data is provided, it must contain the same number of rows as the current state of the table.

        :raises ValueError
        """
        name, data = getargs('name', 'data', kwargs)
        index, table = popargs('index', 'table', kwargs)

        if name in self.__colids:  # column has already been added
            msg = "column '%s' already exists in %s '%s'" % (name, self.__class__.__name__, self.name)
            raise ValueError(msg)

        ckwargs = dict(kwargs)
        cls = VectorData

        # Add table if it's been specified
        if table is not False:
            cls = DynamicTableRegion
            if isinstance(table, DynamicTable):
                ckwargs['table'] = table

        col = cls(**ckwargs)
        col.parent = self
        columns = [col]
        self.__set_table_attr(col)

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
            setattr(self, col_index.name, col_index)

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
            if key in self.__colids:
                ret = self.__df_cols[self.__colids[key]]
            elif key in self.__indices:
                return self.__indices[key]
            else:
                raise KeyError(key)
        else:
            # index by int, list, or slice --> return pandas Dataframe consisting of one or more rows
            # determine the key. If the key is an int, then turn it into a slice to reduce the number of cases below
            arg = key
            if np.issubdtype(type(arg), np.integer):
                arg = np.s_[arg:(arg+1)]
            # index with a python slice (or single integer) to select one or multiple rows
            if isinstance(arg, slice):
                data = OrderedDict()
                for name in self.colnames:
                    col = self.__df_cols[self.__colids[name]]
                    if isinstance(col.data, (Dataset, np.ndarray)) and col.data.ndim > 1:
                        data[name] = [x for x in col[arg]]
                    else:
                        currdata = col[arg]
                        data[name] = currdata
                id_index = self.id.data[arg]
                if np.isscalar(id_index):
                    id_index = [id_index, ]
                ret = pd.DataFrame(data, index=pd.Index(name=self.id.name, data=id_index), columns=self.colnames)
            # index by a list of ints, return multiple rows
            elif isinstance(arg, (tuple, list, np.ndarray)):
                if isinstance(arg, np.ndarray):
                    if len(arg.shape) != 1:
                        raise ValueError("cannot index DynamicTable with multiple dimensions")
                data = OrderedDict()
                for name in self.colnames:
                    col = self.__df_cols[self.__colids[name]]
                    if isinstance(col.data, (Dataset, np.ndarray)) and col.data.ndim > 1:
                        data[name] = [x for x in col[arg]]
                    elif isinstance(col.data, np.ndarray):
                        data[name] = col[arg]
                    else:
                        data[name] = [col[i] for i in arg]
                id_index = (self.id.data[arg]
                            if isinstance(self.id.data, np.ndarray)
                            else [self.id.data[i] for i in arg])
                ret = pd.DataFrame(data, index=pd.Index(name=self.id.name, data=id_index), columns=self.colnames)
            else:
                raise KeyError("Key type not supported by DynamicTable %s" % str(type(arg)))

        return ret

    def __contains__(self, val):
        """
        Check if the given value (i.e., column) exists in this table
        """
        return val in self.__colids or val in self.__indices

    def get(self, key, default=None):
        """
        Get the data for the column specified by key exists, else return default.

        :param key: String with the name of the column
        :param default: Default value to return if the column does not exists
        :return: Result of self[key] (i.e., self.__getitem__(key) if key exists else return default
        """
        if key in self:
            return self[key]
        return default

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
        for idx in self.data:
            if idx < 0 or idx >= len(val):
                raise IndexError('The index ' + str(idx) +
                                 ' is out of range for this DynamicTable of length '
                                 + str(len(val)))
        self.fields['table'] = val

    def __getitem__(self, key):
        """
        Subset the DynamicTableRegion

        :param key: 1) tuple consisting of (str, int) where the string defines the column to select
                       and the int selects the row, 2) int or slice to select a subset of rows

        :return: Result from self.table[....] with the approbritate selection based on the
                 rows selected by this DynamicTableRegion
        """
        # treat the list of indices as data that can be indexed. then pass the
        # result to the table to get the data
        if isinstance(key, tuple):
            arg1 = key[0]
            arg2 = key[1]
            return self.table[self.data[arg1], arg2]
        elif isinstance(key, (int, slice)):
            if isinstance(key, int) and key >= len(self.data):
                raise IndexError('index {} out of bounds for data of length {}'.format(key, len(self.data)))
            return self.table[self.data[key]]
        else:
            raise ValueError("unrecognized argument: '%s'" % key)

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
