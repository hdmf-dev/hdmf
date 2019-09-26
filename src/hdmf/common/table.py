from h5py import Dataset
import numpy as np
import pandas as pd

from ..utils import docval, getargs, ExtenderMeta, call_docval_func, popargs, pystr
from ..container import Container, Data

from . import register_class


@register_class('Index')
class Index(Data):

    __fields__ = ("target",)

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this VectorData'},
            {'name': 'data', 'type': ('array_data', 'data'),
             'doc': 'a dataset where the first dimension is a concatenation of multiple vectors'},
            {'name': 'target', 'type': Data,
             'doc': 'the target dataset that this index applies to'})
    def __init__(self, **kwargs):
        call_docval_func(super(Index, self).__init__, kwargs)


@register_class('VectorData')
class VectorData(Data):

    __fields__ = ("description",)

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this VectorData'},
            {'name': 'description', 'type': str, 'doc': 'a description for this column'},
            {'name': 'data', 'type': ('array_data', 'data'),
             'doc': 'a dataset where the first dimension is a concatenation of multiple vectors', 'default': list()})
    def __init__(self, **kwargs):
        call_docval_func(super(VectorData, self).__init__, kwargs)
        self.description = getargs('description', kwargs)

    @docval({'name': 'val', 'type': None, 'doc': 'the value to add to this column'})
    def add_row(self, **kwargs):
        val = getargs('val', kwargs)
        self.data.append(val)


@register_class('VectorIndex')
class VectorIndex(Index):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this VectorIndex'},
            {'name': 'data', 'type': ('array_data', 'data'),
             'doc': 'a 1D dataset containing indexes that apply to VectorData object'},
            {'name': 'target', 'type': VectorData,
             'doc': 'the target dataset that this index applies to'})
    def __init__(self, **kwargs):
        call_docval_func(super(VectorIndex, self).__init__, kwargs)
        self.target = getargs('target', kwargs)

    def add_vector(self, arg):
        self.target.extend(arg)
        self.data.append(len(self.target))

    def add_row(self, arg):
        self.add_vector(arg)

    def __getitem_helper(self, arg):
        start = 0 if arg == 0 else self.data[arg-1]
        end = self.data[arg]
        return self.target[start:end]

    def __getitem__(self, arg):
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

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this ElementIdentifiers'},
            {'name': 'data', 'type': ('array_data', 'data'), 'doc': 'a 1D dataset containing identifiers',
             'default': list()})
    def __init__(self, **kwargs):
        call_docval_func(super(ElementIdentifiers, self).__init__, kwargs)


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
        '''
        This classmethod will be called during class declaration in the metaclass to automatically
        include all columns declared in subclasses
        '''
        if not isinstance(cls.__columns__, tuple):
            msg = "'__columns__' must be of type tuple, found %s" % type(cls.__columns__)
            raise TypeError(msg)

        if len(bases) and 'DynamicTable' in globals() and issubclass(bases[-1], Container) \
                and bases[-1].__columns__ is not cls.__columns__:
            new_columns = list(cls.__columns__)
            new_columns[0:0] = bases[-1].__columns__
            cls.__columns__ = tuple(new_columns)

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this table'},    # noqa: C901
            {'name': 'description', 'type': str, 'doc': 'a description of what is in this table'},
            {'name': 'id', 'type': ('array_data', ElementIdentifiers), 'doc': 'the identifiers for this table',
             'default': None},
            {'name': 'columns', 'type': (tuple, list), 'doc': 'the columns in this table', 'default': None},
            {'name': 'colnames', 'type': 'array_data', 'doc': 'the names of the columns in this table',
             'default': None})
    def __init__(self, **kwargs):
        id, columns, desc, colnames = popargs('id', 'columns', 'description', 'colnames', kwargs)
        call_docval_func(super(DynamicTable, self).__init__, kwargs)
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
        for col in self.__columns__:
            if col.get('required', False) and col['name'] not in self.__colids:
                self.add_column(col['name'], col['description'],
                                index=col.get('index', False),
                                table=col.get('table', False))

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
        return len(self.id)

    @docval({'name': 'data', 'type': dict, 'doc': 'the data to put in this row', 'default': None},
            {'name': 'id', 'type': int, 'doc': 'the ID for the row', 'default': None},
            allow_extra=True)
    def add_row(self, **kwargs):
        '''
        Add a row to the table. If *id* is not provided, it will auto-increment.
        '''
        data, row_id = popargs('data', 'id', kwargs)
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
                                        table=col.get('table', False))
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
        self.id.data.append(row_id)

        for colname, colnum in self.__colids.items():
            if colname not in data:
                raise ValueError("column '%s' missing" % colname)
            c = self.__df_cols[colnum]
            if isinstance(c, VectorIndex):
                c.add_vector(data[colname])
            else:
                c.add_row(data[colname])

    def __eq__(self, other):
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
        """
        Add a column to this table. If data is provided, it must
        contain the same number of rows as the current state of the table.
        """
        name, data = getargs('name', 'data', kwargs)
        index, table = popargs('index', 'table', kwargs)
        if name in self.__colids:
            msg = "column '%s' already exists in DynamicTable '%s'" % (name, self.name)
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
        region = getargs('region', kwargs)
        if isinstance(region, slice):
            if (region.start is not None and region.start < 0) or (region.stop is not None and region.stop > len(self)):
                msg = 'region slice %s is out of range for this DynamicTable of length ' % (str(region), len(self))
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
        ret = None
        if isinstance(key, tuple):
            # index by row and column, return specific cell
            arg1 = key[0]
            arg2 = key[1]
            if isinstance(arg2, str):
                arg2 = self.__colids[arg2]
            ret = self.__df_cols[arg2][arg1]
        else:
            arg = key
            if isinstance(arg, str):
                # index by one string, return column
                if arg in self.__colids:
                    ret = self.__df_cols[self.__colids[arg]]
                elif arg in self.__indices:
                    return self.__indices[arg]
                else:
                    raise KeyError(arg)
            elif isinstance(arg, (int, np.int8, np.int16, np.int32, np.int64)):
                # index by int, return row
                ret = tuple(col[arg] for col in self.__df_cols)
            elif isinstance(arg, (tuple, list)):
                # index by a list of ints, return multiple rows
                ret = list()
                for i in arg:
                    ret.append(tuple(col[i] for col in self.__df_cols))

        return ret

    def __contains__(self, val):
        return val in self.__colids or val in self.__indices

    def get(self, key, default=None):
        if key in self:
            return self[key]
        return default

    def to_dataframe(self, exclude=set([])):
        '''Produce a pandas DataFrame containing this table's data.
        '''

        data = {}
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
    An object for easily slicing into a DynamicTable
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
        call_docval_func(super(DynamicTableRegion, self).__init__, kwargs)
        self.table = t

    @property
    def table(self):
        return self.fields.get('table')

    @table.setter
    def table(self, val):
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
