import numpy as np

from hdmf.utils import docval, getargs
from hdmf import Container, Data
from hdmf.spec import GroupSpec, DatasetSpec
from hdmf.testing import TestCase
from datetime import datetime, date

from tests.unit.helpers.utils import create_test_type_map


class Bar(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Bar'},
            {'name': 'data', 'type': ('data', 'array_data', datetime, date), 'doc': 'some data'})
    def __init__(self, **kwargs):
        name, data = getargs('name', 'data', kwargs)
        super().__init__(name=name)
        self.__data = data

    @property
    def data_type(self):
        return 'Bar'

    @property
    def data(self):
        return self.__data


class TestBuildDatasetDateTime(TestCase):
    """Test that building a dataset with dtype isodatetime works with datetime and date objects."""

    def _build(self, data, spec_dims=None):
        dataset_kwargs = dict(doc='an example dataset', name='data', dtype='isodatetime')
        if spec_dims is not None:
            dataset_kwargs['dims'] = spec_dims
        bar_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Bar',
            datasets=[DatasetSpec(**dataset_kwargs)],
        )
        type_map = create_test_type_map([bar_spec], {'Bar': Bar})
        return type_map.build(Bar(name='my_bar', data=data)).get('data')

    def test_datetime_scalar(self):
        ret = self._build(datetime(2023, 7, 9))
        assert ret.data == b'2023-07-09T00:00:00'
        assert ret.dtype == 'ascii'

    def test_date_scalar(self):
        ret = self._build(date(2023, 7, 9))
        assert ret.data == b'2023-07-09'
        assert ret.dtype == 'ascii'

    def test_datetime_array(self):
        ret = self._build([datetime(2023, 7, 9), datetime(2023, 7, 10)], spec_dims=(None,))
        assert ret.data == [b'2023-07-09T00:00:00', b'2023-07-10T00:00:00']
        assert ret.dtype == 'ascii'

    def test_date_array(self):
        ret = self._build([date(2023, 7, 9), date(2023, 7, 10)], spec_dims=(None,))
        assert ret.data == [b'2023-07-09', b'2023-07-10']
        assert ret.dtype == 'ascii'

    def test_datetime_object_ndarray(self):
        """An ndarray(dtype=object) of datetimes must serialize with the ISO 8601 'T' separator,
        not the space form that numpy.astype('S') would produce via str(datetime).
        """
        arr = np.array([datetime(2023, 7, 9), datetime(2023, 7, 10)], dtype=object)
        ret = self._build(arr, spec_dims=(None,))
        assert list(ret.data) == [b'2023-07-09T00:00:00', b'2023-07-10T00:00:00']
        assert ret.dtype == 'ascii'

    def test_date_object_ndarray(self):
        arr = np.array([date(2023, 7, 9), date(2023, 7, 10)], dtype=object)
        ret = self._build(arr, spec_dims=(None,))
        assert list(ret.data) == [b'2023-07-09', b'2023-07-10']
        assert ret.dtype == 'ascii'

    def test_string_ndarray(self):
        """An ndarray of pre-formatted ISO strings should pass through astype('S') unchanged."""
        arr = np.array(['2023-07-09T00:00:00', '2023-07-10T00:00:00'])
        ret = self._build(arr, spec_dims=(None,))
        assert list(ret.data) == [b'2023-07-09T00:00:00', b'2023-07-10T00:00:00']
        assert ret.dtype == 'ascii'


class StampedColumn(Data):
    """A typed Data subclass with no fixed dtype, like VectorData."""

    @docval(
        {"name": "name", "type": str, "doc": "name"},
        {"name": "data", "type": ("array_data", "data", datetime, date), "doc": "data"},
    )
    def __init__(self, **kwargs):
        name, data = getargs("name", "data", kwargs)
        super().__init__(name=name, data=data)

    @property
    def data_type(self):
        return "StampedColumn"


class StampedHolder(Container):

    @docval(
        {"name": "name", "type": str, "doc": "name"},
        {"name": "date_of_birth", "type": StampedColumn, "doc": "a typed column"},
    )
    def __init__(self, **kwargs):
        name, date_of_birth = getargs("name", "date_of_birth", kwargs)
        super().__init__(name=name)
        self.__date_of_birth = date_of_birth
        if date_of_birth.parent is None:
            date_of_birth.parent = self

    @property
    def data_type(self):
        return "StampedHolder"

    @property
    def date_of_birth(self):
        return self.__date_of_birth


class TestBuildTypedDatasetDatetime(TestCase):
    """Regression test for #1311.

    A typed dataset (data_type_inc=StampedColumn) whose def-site has no dtype, with the
    inc-site declaring dtype='isodatetime', must accept datetime/date values on build.
    Before this fix, `convert_dtype` failed on raw datetime objects because the typed
    branch (via `__add_containers` -> `build_manager.build`) skips `__convert_string`
    preprocessing.
    """

    def _build(self, holder, dtype):
        col_def = DatasetSpec(doc="a column", data_type_def="StampedColumn")
        col_subspec = DatasetSpec(
            doc="date column", data_type_inc="StampedColumn", name="date_of_birth", dtype=dtype, dims=(None,)
        )
        holder_spec = GroupSpec(doc="a holder", data_type_def="StampedHolder", datasets=[col_subspec])
        type_map = create_test_type_map(
            [col_def, holder_spec],
            {"StampedColumn": StampedColumn, "StampedHolder": StampedHolder},
        )
        return type_map.build(holder)

    def test_typed_isodatetime_array(self):
        col = StampedColumn(name="date_of_birth", data=[datetime(2023, 7, 9), datetime(2023, 7, 10)])
        holder = StampedHolder(name="holder", date_of_birth=col)
        builder = self._build(holder, dtype="isodatetime")
        col_builder = builder.datasets["date_of_birth"]
        assert col_builder.data == [b"2023-07-09T00:00:00", b"2023-07-10T00:00:00"]
        assert col_builder.dtype == "ascii"

    def test_typed_isodatetime_date_array(self):
        col = StampedColumn(name="date_of_birth", data=[date(2023, 7, 9), date(2023, 7, 10)])
        holder = StampedHolder(name="holder", date_of_birth=col)
        builder = self._build(holder, dtype="isodatetime")
        col_builder = builder.datasets["date_of_birth"]
        assert col_builder.data == [b"2023-07-09", b"2023-07-10"]
        assert col_builder.dtype == "ascii"

    def test_typed_datetime_alias(self):
        """The 'datetime' dtype alias maps to the same converter as 'isodatetime'."""
        col = StampedColumn(name="date_of_birth", data=[datetime(2023, 7, 9)])
        holder = StampedHolder(name="holder", date_of_birth=col)
        builder = self._build(holder, dtype="datetime")
        col_builder = builder.datasets["date_of_birth"]
        assert col_builder.data == [b"2023-07-09T00:00:00"]
        assert col_builder.dtype == "ascii"

    def test_typed_datetime_object_ndarray(self):
        """An ndarray(dtype=object) of datetimes through the typed path must serialize with the
        ISO 8601 'T' separator. The typed path skips __convert_string, so this exercises the
        convert_dtype ndarray-of-objects branch directly.
        """
        arr = np.array([datetime(2023, 7, 9), datetime(2023, 7, 10)], dtype=object)
        col = StampedColumn(name="date_of_birth", data=arr)
        holder = StampedHolder(name="holder", date_of_birth=col)
        builder = self._build(holder, dtype="isodatetime")
        col_builder = builder.datasets["date_of_birth"]
        assert list(col_builder.data) == [b"2023-07-09T00:00:00", b"2023-07-10T00:00:00"]
        assert col_builder.dtype == "ascii"

    def test_typed_date_object_ndarray(self):
        arr = np.array([date(2023, 7, 9), date(2023, 7, 10)], dtype=object)
        col = StampedColumn(name="date_of_birth", data=arr)
        holder = StampedHolder(name="holder", date_of_birth=col)
        builder = self._build(holder, dtype="isodatetime")
        col_builder = builder.datasets["date_of_birth"]
        assert list(col_builder.data) == [b"2023-07-09", b"2023-07-10"]
        assert col_builder.dtype == "ascii"

    def test_typed_string_ndarray(self):
        """An ndarray of pre-formatted ISO strings through the typed path takes the non-object
        branch (value.astype('S')) in convert_dtype.
        """
        arr = np.array(["2023-07-09T00:00:00", "2023-07-10T00:00:00"])
        col = StampedColumn(name="date_of_birth", data=arr)
        holder = StampedHolder(name="holder", date_of_birth=col)
        builder = self._build(holder, dtype="isodatetime")
        col_builder = builder.datasets["date_of_birth"]
        assert list(col_builder.data) == [b"2023-07-09T00:00:00", b"2023-07-10T00:00:00"]
        assert col_builder.dtype == "ascii"
