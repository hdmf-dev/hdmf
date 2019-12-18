import numpy as np
import xarray as xr
import unittest

from hdmf.container import AbstractContainer, Container, Data, Coordinates
from hdmf.testing import TestCase


class Subcontainer(Container):
    pass


class TestContainer(TestCase):

    def test_constructor(self):
        """Test that constructor properly sets parent and both child and parent have an object_id
        """
        parent_obj = Container('obj1')
        child_obj = Container.__new__(Container, parent=parent_obj)
        self.assertIs(child_obj.parent, parent_obj)
        self.assertIs(parent_obj.children[0], child_obj)
        self.assertIsNotNone(parent_obj.object_id)
        self.assertIsNotNone(child_obj.object_id)

    def test_constructor_object_id_none(self):
        """Test that setting object_id to None in __new__ is OK and the object ID is set on get
        """
        parent_obj = Container('obj1')
        child_obj = Container.__new__(Container, parent=parent_obj, object_id=None)
        self.assertIsNotNone(child_obj.object_id)

    def test_set_parent(self):
        """Test that parent setter properly sets parent
        """
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        child_obj.parent = parent_obj
        self.assertIs(child_obj.parent, parent_obj)
        self.assertIs(parent_obj.children[0], child_obj)

    def test_set_parent_overwrite(self):
        """Test that parent setter properly blocks overwriting
        """
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        child_obj.parent = parent_obj
        self.assertIs(parent_obj.children[0], child_obj)

        another_obj = Container('obj3')
        with self.assertRaisesWith(ValueError,
                                   'Cannot reassign parent to: %s. Parent is already: %s.'
                                   % (repr(child_obj), repr(child_obj.parent))):
            child_obj.parent = another_obj
        self.assertIs(child_obj.parent, parent_obj)
        self.assertIs(parent_obj.children[0], child_obj)

    def test_set_parent_overwrite_proxy(self):
        """Test that parent setter properly blocks overwriting with proxy/object
        """
        child_obj = Container('obj2')
        child_obj.parent = object()

        with self.assertRaisesRegex(ValueError,
                                    r"Got None for parent of '[^/]+' - cannot overwrite Proxy with NoneType"):
            child_obj.parent = None

    def test_slash_restriction(self):
        self.assertRaises(ValueError, Container, 'bad/name')

    def test_set_modified_parent(self):
        """Test that set modified properly sets parent modified
        """
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        child_obj.parent = parent_obj
        parent_obj.set_modified(False)
        child_obj.set_modified(False)
        self.assertFalse(child_obj.parent.modified)
        child_obj.set_modified()
        self.assertTrue(child_obj.parent.modified)

    def test_add_child(self):
        """Test that add child creates deprecation warning and also properly sets child's parent and modified
        """
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        parent_obj.set_modified(False)
        with self.assertWarnsWith(DeprecationWarning, 'add_child is deprecated. Set the parent attribute instead.'):
            parent_obj.add_child(child_obj)
        self.assertIs(child_obj.parent, parent_obj)
        self.assertTrue(parent_obj.modified)
        self.assertIs(parent_obj.children[0], child_obj)

    def test_set_parent_exists(self):
        """Test that setting a parent a second time does nothing
        """
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        child_obj3 = Container('obj3')
        child_obj.parent = parent_obj
        child_obj.parent = parent_obj
        child_obj3.parent = parent_obj
        self.assertEqual(len(parent_obj.children), 2)
        self.assertIs(parent_obj.children[0], child_obj)
        self.assertIs(parent_obj.children[1], child_obj3)

    def test_reassign_container_source(self):
        """Test that reassign container source throws error
        """
        parent_obj = Container('obj1')
        parent_obj.container_source = 'a source'
        with self.assertRaisesWith(Exception, 'cannot reassign container_source'):
            parent_obj.container_source = 'some other source'

    def test_repr(self):
        parent_obj = Container('obj1')
        self.assertRegex(str(parent_obj), r"obj1 hdmf.container.Container at 0x%d" % id(parent_obj))

    def test_type_hierarchy(self):
        self.assertEqual(Container.type_hierarchy(), (Container, AbstractContainer, object))
        self.assertEqual(Subcontainer.type_hierarchy(), (Subcontainer, Container, AbstractContainer, object))


class Bar(Container):

    __fields__ = ('data1', 'data2', 'data3')

    def __init__(self, name, data1, data2, data3=None):
        super().__init__(name=name)
        self.data1 = data1
        self.data2 = data2
        self.data3 = data3


class TestContainerDims(TestCase):

    def test_get_no_dims(self):
        obj1 = Bar('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        self.assertDictEqual(obj1.dims, {})

    def test_set_dims_1d(self):
        obj1 = Bar('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        obj1.set_dims(array_name='data1', dims=('numbers', ))
        self.assertDictEqual(obj1.dims, {'data1': ('numbers', )})

    def test_set_dims_3d(self):
        obj1 = Bar('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        obj1.set_dims(array_name='data2', dims=('x', 'y', 'z'))
        self.assertDictEqual(obj1.dims, {'data2': ('x', 'y', 'z')})

    def test_set_dims_dataio(self):
        # TODO
        raise unittest.SkipTest('TODO')

    def test_set_dims_dci(self):
        # TODO
        raise unittest.SkipTest('TODO')

    def test_set_dims_h5dataset(self):
        # TODO
        raise unittest.SkipTest('TODO')

    def test_set_dims_empty(self):
        obj1 = Bar('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        msg = "Number of dims must equal number of axes for field 'data1' in Bar 'obj1' (0 != 1)."
        with self.assertRaisesWith(ValueError, msg):
            obj1.set_dims(array_name='data1', dims=tuple())

    def test_set_dims_too_many(self):
        obj1 = Bar('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        msg = "Number of dims must equal number of axes for field 'data1' in Bar 'obj1' (2 != 1)."
        with self.assertRaisesWith(ValueError, msg):
            obj1.set_dims(array_name='data1', dims=('numbers', 'dup'))

    def test_set_dims_unknown_name(self):
        obj1 = Bar('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        msg = "Field named 'data4' not found in Bar 'obj1'."
        with self.assertRaisesWith(ValueError, msg):
            obj1.set_dims(array_name='data4', dims=('numbers', ))

    def test_set_dims_array_none(self):
        """Test that set_dims raises an error if given an array name that is defined on the class but not set."""
        obj1 = Bar('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        msg = "Field named 'data3' not found in Bar 'obj1'."
        with self.assertRaisesWith(ValueError, msg):
            obj1.set_dims(array_name='data3', dims=('numbers', ))

    def test_set_dim_axis_non_array(self):
        obj1 = Bar('obj1', data1='hello', data2=np.arange(20).reshape((2, 5, 2)))
        msg = "Cannot determine shape of field 'data1' in Bar 'obj1'."
        with self.assertRaisesWith(ValueError, msg):
            obj1.set_dims(array_name='data1', dims=('numbers', ))

    def test_set_dims_dup_name(self):
        obj1 = Bar('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        obj1.set_dims(array_name='data1', dims=('numbers', ))
        msg = "Cannot reset dims for field 'data1' in Bar 'obj1'. Dims is already ('numbers',)."
        with self.assertRaisesWith(ValueError, msg):
            obj1.set_dims(array_name='data1', dims=('numbers', ))

    def test_set_dims_dup_dim_name(self):
        obj1 = Bar('obj1', data1=[1, 2, 3], data2=np.arange(20).reshape((2, 5, 2)))
        msg = "Cannot set dims for field 'data1' in Bar 'obj1'. Dim names must be unique."
        with self.assertRaisesWith(ValueError, msg):
            obj1.set_dims(array_name='data1', dims=('numbers', 'numbers'))


class TestContainerCoords(TestCase):

    def test_get_coord_none(self):
        obj1 = Bar('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        self.assertDictEqual(obj1.coords, {})

    def test_set_coord(self):
        obj1 = Bar('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        obj1.set_dims(array_name='data1', dims=('x', ))
        obj1.set_coord(array_name='data1', name='letters', axes=(0, ), coord_array_name='data2',
                       coord_array_axes=(0, ), coord_type='aligned')

        self.assertEqual(len(obj1.coords), 1)
        received_coords = obj1.coords['data1']
        self.assertIsInstance(received_coords, Coordinates)
        self.assertIs(received_coords.parent, obj1)
        self.assertEqual(received_coords['letters'], Coordinates.Coord(name='letters', dims=('x', ),
                                                                       coord_array=obj1.data2,
                                                                       coord_array_axes=(0, ), coord_type='aligned'))

    def test_set_coord_dataio(self):
        # TODO
        raise unittest.SkipTest('TODO')

    def test_set_coord_dci(self):
        # TODO
        raise unittest.SkipTest('TODO')

    def test_set_coord_h5dataset(self):
        # TODO
        raise unittest.SkipTest('TODO')

    # TODO catch all the ValueErrors from set_coord

    def test_to_xarray_dataarray(self):
        obj1 = Bar('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        obj1.set_dims(array_name='data1', dims=('x', ))
        obj1.set_coord(array_name='data1', name='letters', axes=(0, ), coord_array_name='data2',
                       coord_array_axes=(0, ), coord_type='aligned')

        arr = obj1.to_xarray_dataarray(array_name='data1')
        expected = xr.DataArray([1, 2, 3], dims=('x', ), coords={'letters': (('x', ), ['a', 'b', 'c'])})
        xr.testing.assert_equal(arr, expected)

    def test_to_xarray_dataarray_unknown_name(self):
        obj1 = Bar('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        obj1.set_dims(array_name='data1', dims=('x', ))
        obj1.set_coord(array_name='data1', name='letters', axes=(0, ), coord_array_name='data2',
                       coord_array_axes=(0, ), coord_type='aligned')
        with self.assertRaisesWith(ValueError, "Field name 'data3' not found in Bar 'obj1'."):
            obj1.to_xarray_dataarray(array_name='data3')

    def test_to_xarray_dataarray_coord_not_all_axes(self):
        obj1 = Bar('obj1', data1=[1, 2, 3], data2=[['a', 'b'], ['c', 'd'], ['e', 'f']])
        obj1.set_dims(array_name='data1', dims=('x', ))
        obj1.set_coord(array_name='data1', name='letters', axes=(0, ), coord_array_name='data2',
                       coord_array_axes=(0, ), coord_type='aligned')
        msg = ("Cannot convert the array 'data1' to an xarray.DataArray. All coordinate arrays must map all of their "
               "axes to a set of axes on 'data1'.")
        with self.assertRaisesWith(ValueError, msg):
            obj1.to_xarray_dataarray(array_name='data1')

    def test_to_xarray_dataarray_no_coord(self):
        obj1 = Bar('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        obj1.set_dims(array_name='data1', dims=('x', ))
        arr = obj1.to_xarray_dataarray(array_name='data1')
        expected = xr.DataArray([1, 2, 3], dims=('x', ))
        xr.testing.assert_equal(arr, expected)

    def test_to_xarray_dataarray_no_dim(self):
        obj1 = Bar('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        arr = obj1.to_xarray_dataarray(array_name='data1')
        expected = xr.DataArray([1, 2, 3])
        xr.testing.assert_equal(arr, expected)


class TestData(TestCase):

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

    def test_shape_nparray(self):
        """
        Test that shape works for np.array
        """
        data_obj = Data('my_data', np.arange(10).reshape(2, 5))
        self.assertTupleEqual(data_obj.shape, (2, 5))

    def test_shape_list(self):
        """
        Test that shape works for np.array
        """
        data_obj = Data('my_data', [[0, 1, 2, 3, 4], [0, 1, 2, 3, 4]])
        self.assertTupleEqual(data_obj.shape, (2, 5))


class TestCoordinates(TestCase):

    def test_constructor(self):
        """Test that the Coordinates constructor sets values correctly"""
        obj = Container('obj1')
        coords = Coordinates(obj)
        self.assertIs(coords.parent, obj)
        self.assertEqual(list(coords.values()), [])

    def test_add_getitem(self):
        """Test that adding a coord to Coordinates and accessing it works"""
        obj = Container('obj1')
        coords = Coordinates(obj)
        coords.add(name='my_coord', dims=('x', ), coord_array=[0, 1, 2, 3, 4], coord_array_axes=(0, ),
                   coord_type='aligned')

        expected = Coordinates.Coord(name='my_coord', dims=('x', ), coord_array=[0, 1, 2, 3, 4], coord_array_axes=(0, ),
                                     coord_type='aligned')
        self.assertEqual(coords['my_coord'], expected)

    def test_add_dup(self):
        """Test that adding a coord whose name is already in Coordinates raises an error"""
        obj = Container('obj1')
        coords = Coordinates(obj)
        coords.add(name='my_coord', dims=('x', ), coord_array=[0, 1, 2, 3, 4], coord_array_axes=(0, ),
                   coord_type='aligned')

        msg = "Coordinate 'my_coord' already exists. Cannot overwrite values in Coordinates."
        with self.assertRaisesWith(ValueError, msg):
            coords.add(name='my_coord', dims=('y', ), coord_array=[0, 1, 2, 3, 4], coord_array_axes=(0, ),
                       coord_type='aligned')

    def test_eq(self):
        """Test equality of Coordinates"""
        obj = Container('obj1')
        coords = Coordinates(obj)
        coords.add(name='my_coord', dims=('x', ), coord_array=[0, 1, 2, 3, 4], coord_array_axes=(0, ),
                   coord_type='aligned')

        coords2 = Coordinates(obj)
        coords2.add(name='my_coord', dims=('x', ), coord_array=[0, 1, 2, 3, 4], coord_array_axes=(0, ),
                    coord_type='aligned')
        self.assertEqual(coords, coords2)

    def test_not_eq(self):
        """Test correct failure of equality of Coordinates"""
        obj = Container('obj1')
        coords = Coordinates(obj)
        coords.add(name='my_coord', dims=('x', ), coord_array=[0, 1, 2, 3, 4], coord_array_axes=(0, ),
                   coord_type='aligned')

        coords2 = Coordinates(obj)
        coords2.add(name='my_coord', dims=('y', ), coord_array=[0, 1, 2, 3, 4], coord_array_axes=(0, ),
                    coord_type='aligned')
        self.assertNotEqual(coords, coords2)

    def test_dict(self):
        """Test a variety of dictionary methods on Coordinates"""
        obj = Container('obj1')
        coords = Coordinates(obj)
        coords.add(name='my_coord', dims=('x', ), coord_array=[0, 1, 2, 3, 4], coord_array_axes=(0, ),
                   coord_type='aligned')

        expected_coord = Coordinates.Coord(name='my_coord', dims=('x', ), coord_array=[0, 1, 2, 3, 4],
                                           coord_array_axes=(0, ), coord_type='aligned')

        for k, v in coords.items():
            self.assertEqual(k, 'my_coord')
            self.assertEqual(v, expected_coord)

        self.assertEqual(len(coords), 1)
        self.assertEqual(str(coords), "{'my_coord': Coord(name='my_coord', dims=('x',), coord_array=[0, 1, 2, 3, 4], "
                                      "coord_array_axes=(0,), coord_type='aligned')}")
        self.assertEqual(list(coords.keys()), ['my_coord'])
        self.assertEqual(list(coords.values()), [expected_coord])
        self.assertEqual(list(iter(coords)), ['my_coord'])
