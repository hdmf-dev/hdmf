import unittest
import numpy as np

from hdmf.container import AbstractContainer, Container, Data


class TestAbstractContainer(unittest.TestCase):

    def test_constructor(self):
        """Test that constructor properly sets parent and both child and parent have an object_id
        """
        parent_obj = AbstractContainer('obj1')
        child_obj = AbstractContainer.__new__(AbstractContainer, parent=parent_obj)
        self.assertIs(child_obj.parent, parent_obj)
        self.assertIs(parent_obj.children[0], child_obj)
        self.assertIsNotNone(parent_obj.object_id)
        self.assertIsNotNone(child_obj.object_id)

    def test_constructor_object_id_none(self):
        """Test that setting object_id to None in __new__ is OK and the object ID is set on get
        """
        parent_obj = AbstractContainer('obj1')
        child_obj = AbstractContainer.__new__(AbstractContainer, parent=parent_obj, object_id=None)
        self.assertIsNotNone(child_obj.object_id)

    def test_set_parent(self):
        """Test that parent setter properly sets parent
        """
        parent_obj = AbstractContainer('obj1')
        child_obj = AbstractContainer('obj2')
        child_obj.parent = parent_obj
        self.assertIs(child_obj.parent, parent_obj)
        self.assertIs(parent_obj.children[0], child_obj)

    def test_set_parent_overwrite(self):
        """Test that parent setter properly blocks overwriting
        """
        parent_obj = AbstractContainer('obj1')
        child_obj = AbstractContainer('obj2')
        child_obj.parent = parent_obj
        self.assertIs(parent_obj.children[0], child_obj)

        another_obj = AbstractContainer('obj3')
        with self.assertRaisesRegex(ValueError,
                                    'Cannot reassign parent to AbstractContainer: %s. Parent is already: %s.'
                                    % (repr(child_obj), repr(child_obj.parent))):
            child_obj.parent = another_obj
        self.assertIs(child_obj.parent, parent_obj)
        self.assertIs(parent_obj.children[0], child_obj)

    def test_set_parent_overwrite_proxy(self):
        """Test that parent setter properly blocks overwriting with proxy/object
        """
        child_obj = AbstractContainer('obj2')
        child_obj.parent = object()

        with self.assertRaisesRegex(ValueError,
                                    r"Got None for parent of '[^/]+' - cannot overwrite Proxy with NoneType"):
            child_obj.parent = None

    def test_slash_restriction(self):
        self.assertRaises(ValueError, AbstractContainer, 'bad/name')

    def test_set_modified_parent(self):
        """Test that set modified properly sets parent modified
        """
        parent_obj = AbstractContainer('obj1')
        child_obj = AbstractContainer('obj2')
        child_obj.parent = parent_obj
        parent_obj.set_modified(False)
        child_obj.set_modified(False)
        self.assertFalse(child_obj.parent.modified)
        child_obj.set_modified()
        self.assertTrue(child_obj.parent.modified)

    def test_add_child(self):
        """Test that add child creates deprecation warning and also properly sets child's parent and modified
        """
        parent_obj = AbstractContainer('obj1')
        child_obj = AbstractContainer('obj2')
        parent_obj.set_modified(False)
        with self.assertWarnsRegex(DeprecationWarning,
                                   r'add_child is deprecated\. Set the parent attribute instead\.'):
            parent_obj.add_child(child_obj)
        self.assertIs(child_obj.parent, parent_obj)
        self.assertTrue(parent_obj.modified)
        self.assertIs(parent_obj.children[0], child_obj)

    def test_set_parent_exists(self):
        """Test that setting a parent a second time does nothing
        """
        parent_obj = AbstractContainer('obj1')
        child_obj = AbstractContainer('obj2')
        child_obj3 = AbstractContainer('obj3')
        child_obj.parent = parent_obj
        child_obj.parent = parent_obj
        child_obj3.parent = parent_obj
        self.assertEqual(len(parent_obj.children), 2)
        self.assertIs(parent_obj.children[0], child_obj)
        self.assertIs(parent_obj.children[1], child_obj3)

    def test_reassign_container_source(self):
        """Test that reassign container source throws error
        """
        parent_obj = AbstractContainer('obj1')
        parent_obj.container_source = 'a source'
        with self.assertRaisesRegex(Exception, 'cannot reassign container_source'):
            parent_obj.container_source = 'some other source'

    def test_type_hierarchy(self):
        self.assertEqual(AbstractContainer.type_hierarchy(), (AbstractContainer, object))


class Subcontainer(Container):

    __fields__ = ('data1', 'data2', 'data3')

    def __init__(self, name, data1, data2, data3=None):
        super(Subcontainer, self).__init__(name=name)
        self.data1 = data1
        self.data2 = data2
        self.data3 = data3


class TestContainer(unittest.TestCase):

    def test_repr(self):
        parent_obj = Container('obj1')
        self.assertRegex(str(parent_obj), r"obj1 hdmf.container.Container at 0x%d" % id(parent_obj))


class TestContainerDims(unittest.TestCase):

    def test_get_dims(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        self.assertDictEqual(obj1.dims, {})

    def test_set_dim_axis0(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        obj1.set_dim(data_name='data1', axis=0, dim='numbers')
        self.assertDictEqual(obj1.dims, {'data1': ['numbers']})

    def test_set_dim_axis1(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        obj1.set_dim(data_name='data2', axis=1, dim='numbers')
        self.assertDictEqual(obj1.dims, {'data2': ['dim_0', 'numbers', 'dim_2']})
        obj1.set_dim(data_name='data2', axis=2, dim='more numbers')
        self.assertDictEqual(obj1.dims, {'data2': ['dim_0', 'numbers', 'more numbers']})
        obj1.set_dim(data_name='data2', axis=1, dim='awesome numbers')
        self.assertDictEqual(obj1.dims, {'data2': ['dim_0', 'awesome numbers', 'more numbers']})

    def test_set_dim_dataio(self):
        # TODO
        raise unittest.SkipTest('TODO')

    def test_set_dim_dci(self):
        # TODO
        raise unittest.SkipTest('TODO')

    def test_set_dim_h5dataset(self):
        # TODO
        raise unittest.SkipTest('TODO')

    def test_set_dim_empty_dim(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        obj1.set_dim(data_name='data1', axis=0, dim='')
        self.assertDictEqual(obj1.dims, {'data1': ['']})

    def test_set_dim_unknown_name(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        with self.assertRaisesRegex(ValueError, r"No field named 'data4' in Subcontainer\."):
            obj1.set_dim(data_name='data4', axis=0, dim='numbers')

    def test_set_dim_axis_non_array(self):
        obj1 = Subcontainer('obj1', data1='hello', data2=np.arange(20).reshape((2, 5, 2)))
        with self.assertRaisesRegex(ValueError, r"Cannot determine shape of field 'data1' in Subcontainer\."):
            obj1.set_dim(data_name='data1', axis=0, dim='numbers')

    def test_set_dim_axis_negative(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        with self.assertRaisesRegex(ValueError, r"Axis -1 does not exist for field 'data1' in Subcontainer\."):
            obj1.set_dim(data_name='data1', axis=-1, dim='numbers')

    def test_set_dim_axis_over_bounds(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        with self.assertRaisesRegex(ValueError, r"Axis 1 does not exist for field 'data1' in Subcontainer\."):
            obj1.set_dim(data_name='data1', axis=1, dim='numbers')

    def test_set_dim_dup_name(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        obj1.set_dim(data_name='data2', axis=1, dim='numbers')
        with self.assertRaisesRegex(ValueError, (r"Cannot set dim 'numbers' for axis 2 of field data2 in Subcontainer\."
                                                 r" Dim 'numbers' is already used for axis 1\.")):
            obj1.set_dim(data_name='data2', axis=2, dim='numbers')

    def test_set_dim_dup_name_ok(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        obj1.set_dim(data_name='data2', axis=1, dim='numbers')
        obj1.set_dim(data_name='data2', axis=1, dim='numbers')
        self.assertDictEqual(obj1.dims, {'data2': ['dim_0', 'numbers', 'dim_2']})

    def test_get_dim_axis(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        obj1.set_dim(data_name='data2', axis=1, dim='numbers')
        ax = obj1._get_dim_axis(data_name='data2', dim='numbers')
        self.assertEqual(ax, 1)

    def test_get_dim_axis_unknown_name(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        with self.assertRaisesRegex(ValueError, r"No field named 'data4' in Subcontainer\."):
            obj1._get_dim_axis(data_name='data4', dim='numbers')

    def test_get_dim_axis_no_dims(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        ax = obj1._get_dim_axis(data_name='data2', dim='numbers')
        self.assertIsNone(ax)

    def test_get_dim_axis_unknown_dim(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        obj1.set_dim(data_name='data2', axis=1, dim='numbers')
        with self.assertRaisesRegex(ValueError, r"Dim name 'letters' not found for field 'data2' of Subcontainer\."):
            obj1._get_dim_axis(data_name='data2', dim='letters')


class TestContainerCoords(unittest.TestCase):

    def test_set_get_coord(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        obj1.set_coord('data1', 0, 'letters', 'data2')
        self.assertEqual(obj1.get_coord('data1', 0, 'letters'), obj1.data2)

    def test_set_coord_data_not_found(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        with self.assertRaisesRegex(ValueError, "Field name 'data0' not found in Subcontainer"):
            obj1.set_coord('data0', 0, 'letters', 'data2')

    def test_set_coord_coord_not_found(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        with self.assertRaisesRegex(ValueError, "Coord name 'data3' not found in Subcontainer"):
            obj1.set_coord('data1', 0, 'letters', 'data3')

    def test_set_coord_out_bounds(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        with self.assertRaisesRegex(ValueError, 'Axis 1 does not exist for coords of data1 in Subcontainer'):
            obj1.set_coord('data1', 1, 'letters', 'data2')
        with self.assertRaisesRegex(ValueError, 'Axis -1 does not exist for coords of data1 in Subcontainer'):
            obj1.set_coord('data1', -1, 'letters', 'data2')

    def test_set_coord_exists(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'], data3=['A', 'B', 'C'])
        obj1.set_coord('data1', 0, 'letters', 'data2')
        obj1.set_coord('data1', 0, 'letters', 'data3')
        self.assertEqual(obj1.get_coord('data1', 0, 'letters'), obj1.data3)

    def test_get_coord_data_not_found(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        obj1.set_coord('data1', 0, 'letters', 'data2')
        with self.assertRaisesRegex(ValueError, "Field name 'data0' not found in Subcontainer"):
            obj1.get_coord('data0', 0, 'letters')

    def test_get_coord_coord_not_found(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        obj1.set_coord('data1', 0, 'letters', 'data2')
        with self.assertRaisesRegex(ValueError, "Coord label 'symbols' not found in Subcontainer"):
            obj1.get_coord('data1', 0, 'symbols')

    def test_get_coord_out_bounds(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        obj1.set_coord('data1', 0, 'letters', 'data2')
        with self.assertRaisesRegex(ValueError, 'Axis 1 does not exist for coords of data1 in Subcontainer'):
            obj1.get_coord('data1', 1, 'letters')
        with self.assertRaisesRegex(ValueError, 'Axis -1 does not exist for coords of data1 in Subcontainer'):
            obj1.get_coord('data1', -1, 'letters')

    def test_test(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        obj1.set_coord('data1', 0, 'letters', 'data2', '234324', 345)  # TODO should trigger extra argument error?


class TestData(unittest.TestCase):

    def test_bool_true(self):
        """Test that __bool__ method works correctly on data with len
        """
        data_obj = Data('my_data', [1, 2, 3, 4, 5])
        self.assertTrue(data_obj)

    def test_bool_false(self):
        """Test that __bool__ method works correctly on empty data
        """
        data_obj = Data('my_data', [])
        self.assertFalse(data_obj)


if __name__ == '__main__':
    unittest.main()
