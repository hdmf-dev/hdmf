import pandas as pd

from hdmf.container import Table, Row, RowGetter
from hdmf.testing import TestCase


class TestTable(TestCase):

    @classmethod
    def get_table_class(cls):
        class MyTable(Table):

            __defaultname__ = 'my_table'

            __columns__ = [
                {'name': 'col1', 'type': str, 'help': 'a string column'},
                {'name': 'col2', 'type': int, 'help': 'an integer column'},
            ]
        return MyTable

    def test_init(self):
        MyTable = TestTable.get_table_class()
        table = MyTable('test_table')
        self.assertTrue(hasattr(table, '__colidx__'))
        self.assertEqual(table.__colidx__, {'col1': 0, 'col2': 1})

    def test_add_row_getitem(self):
        MyTable = TestTable.get_table_class()
        table = MyTable('test_table')
        table.add_row(col1='foo', col2=100)
        table.add_row(col1='bar', col2=200)
        row1 = table[0]
        row2 = table[1]
        self.assertEqual(row1, ('foo', 100))
        self.assertEqual(row2, ('bar', 200))

    def test_to_dataframe(self):
        MyTable = TestTable.get_table_class()
        table = MyTable('test_table')
        table.add_row(col1='foo', col2=100)
        table.add_row(col1='bar', col2=200)

        df = table.to_dataframe()
        exp = pd.DataFrame(data=[{'col1': 'foo', 'col2': 100}, {'col1': 'bar', 'col2': 200}])
        pd.testing.assert_frame_equal(df, exp)

    def test_from_dataframe(self):
        MyTable = TestTable.get_table_class()
        exp = pd.DataFrame(data=[{'col1': 'foo', 'col2': 100}, {'col1': 'bar', 'col2': 200}])
        table = MyTable.from_dataframe(exp)
        row1 = table[0]
        row2 = table[1]
        self.assertEqual(row1, ('foo', 100))
        self.assertEqual(row2, ('bar', 200))


class TestRow(TestCase):

    def setUp(self):
        self.MyTable = TestTable.get_table_class()

        class MyRow(Row):
            __table__ = self.MyTable

        self.MyRow = MyRow

        self.table = self.MyTable('test_table')

    def test_row_no_table(self):
        with self.assertRaisesRegex(ValueError, '__table__ must be set if sub-classing Row'):
            class MyRow(Row):
                pass

    def test_table_init(self):
        MyTable = TestTable.get_table_class()
        table = MyTable('test_table')
        self.assertFalse(hasattr(table, 'row'))

        table_w_row = self.MyTable('test_table')
        self.assertTrue(hasattr(table_w_row, 'row'))
        self.assertIsInstance(table_w_row.row, RowGetter)
        self.assertIs(table_w_row.row.table, table_w_row)

    def test_init(self):
        row1 = self.MyRow(col1='foo', col2=100, table=self.table)

        # make sure Row object set up properly
        self.assertEqual(row1.idx, 0)
        self.assertEqual(row1.col1, 'foo')
        self.assertEqual(row1.col2, 100)

        # make sure Row object is stored in Table peroperly
        tmp_row1 = self.table.row[0]
        self.assertEqual(tmp_row1, row1)

    def test_add_row_getitem(self):
        self.table.add_row(col1='foo', col2=100)
        self.table.add_row(col1='bar', col2=200)

        row1 = self.table.row[0]
        self.assertIsInstance(row1, self.MyRow)
        self.assertEqual(row1.idx, 0)
        self.assertEqual(row1.col1, 'foo')
        self.assertEqual(row1.col2, 100)

        row2 = self.table.row[1]
        self.assertIsInstance(row2, self.MyRow)
        self.assertEqual(row2.idx, 1)
        self.assertEqual(row2.col1, 'bar')
        self.assertEqual(row2.col2, 200)

        # test memoization
        row3 = self.table.row[0]
        self.assertIs(row3, row1)

    def test_todict(self):
        row1 = self.MyRow(col1='foo', col2=100, table=self.table)
        self.assertEqual(row1.todict(), {'col1': 'foo', 'col2': 100})
