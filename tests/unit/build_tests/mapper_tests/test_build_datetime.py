from hdmf.utils import docval, getargs
from hdmf import Container
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
