from hdmf.common import DynamicTable, VectorData, \
    DynamicTableRegion, SimpleMultiContainer
from hdmf.testing import TestCase, H5RoundTripMixin


class DynamicTableRegionRoundTrip(H5RoundTripMixin, TestCase):

    def make_tables(self):
        self.spec2 = [
            {'name': 'qux', 'description': 'qux column'},
            {'name': 'quz', 'description': 'quz column'},
        ]
        self.data2 = [
            ['qux_1', 'qux_2'],
            ['quz_1', 'quz_2'],
        ]

        target_columns = [
            VectorData(name=s['name'], description=s['description'], data=d)
            for s, d in zip(self.spec2, self.data2)
        ]
        target_table = DynamicTable("target_table", 'example table to target with a DynamicTableRegion',
                                    columns=target_columns)

        self.spec1 = [
            {'name': 'foo', 'description': 'foo column'},
            {'name': 'bar', 'description': 'bar column'},
            {'name': 'baz', 'description': 'baz column'},
            {'name': 'dtr', 'description': 'DTR'},
        ]
        self.data1 = [
            [1, 2, 3, 4, 5],
            [10.0, 20.0, 30.0, 40.0, 50.0],
            ['cat', 'dog', 'bird', 'fish', 'lizard']
        ]
        columns = [
            VectorData(name=s['name'], description=s['description'], data=d)
            for s, d in zip(self.spec1, self.data1)
        ]
        columns.append(DynamicTableRegion(name='dtr', description='example DynamicTableRegion',
                                          data=[0, 1, 1, 0, 1], table=target_table))
        table = DynamicTable("table_with_dtr", 'a test table that has a DynamicTableRegion', columns=columns)
        vdata = VectorData('vector_data', 'a test Data object', data=[-1, -2, -3, -4, -5])
        return table, target_table, vdata

    def setUp(self):
        self.table, self.target_table, self.vdata = self.make_tables()
        super().setUp()

    def setUpContainer(self):
        multi_container = SimpleMultiContainer('multi', [self.table, self.target_table, self.vdata])
        return multi_container

    def setUpExtras(self):
        return [self.target_table]
