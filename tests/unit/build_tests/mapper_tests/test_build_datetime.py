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

    def test_datetime_scalar(self):
        bar_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Bar',
            datasets=[DatasetSpec(doc='an example dataset', name='data', dtype='isodatetime')],
        )
        type_map = create_test_type_map([bar_spec], {'Bar': Bar})

        bar_inst = Bar(name='my_bar', data=datetime(2023, 7, 9))
        builder = type_map.build(bar_inst)
        ret = builder.get('data')
        assert ret.data == b'2023-07-09T00:00:00'
        assert ret.dtype == 'ascii'

    def test_date_scalar(self):
        bar_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Bar',
            datasets=[DatasetSpec(doc='an example dataset', name='data', dtype='isodatetime')],
        )
        type_map = create_test_type_map([bar_spec], {'Bar': Bar})

        bar_inst = Bar(name='my_bar', data=date(2023, 7, 9))
        builder = type_map.build(bar_inst)
        ret = builder.get('data')
        assert ret.data == b'2023-07-09'
        assert ret.dtype == 'ascii'

    def test_datetime_array(self):
        bar_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Bar',
            datasets=[DatasetSpec(doc='an example dataset', name='data', dtype='isodatetime', dims=(None,))],
        )
        type_map = create_test_type_map([bar_spec], {'Bar': Bar})

        bar_inst = Bar(name='my_bar', data=[datetime(2023, 7, 9), datetime(2023, 7, 10)])
        builder = type_map.build(bar_inst)
        ret = builder.get('data')
        assert ret.data == [b'2023-07-09T00:00:00', b'2023-07-10T00:00:00']
        assert ret.dtype == 'ascii'

    def test_date_array(self):
        bar_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Bar',
            datasets=[DatasetSpec(doc='an example dataset', name='data', dtype='isodatetime', dims=(None,))],
        )
        type_map = create_test_type_map([bar_spec], {'Bar': Bar})

        bar_inst = Bar(name='my_bar', data=[date(2023, 7, 9), date(2023, 7, 10)])
        builder = type_map.build(bar_inst)
        ret = builder.get('data')
        assert ret.data == [b'2023-07-09', b'2023-07-10']
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
