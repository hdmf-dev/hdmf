import unittest2 as unittest

from hdmf.data_utils import DataIO
import numpy as np
from copy import copy, deepcopy


class DataIOTests(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_copy(self):
        obj = DataIO(data=[1., 2., 3.])
        obj_copy = copy(obj)
        self.assertNotEqual(id(obj), id(obj_copy))
        self.assertEqual(id(obj.data), id(obj_copy.data))

    def test_deepcopy(self):
        obj = DataIO(data=[1., 2., 3.])
        obj_copy = deepcopy(obj)
        self.assertNotEqual(id(obj), id(obj_copy))
        self.assertNotEqual(id(obj.data), id(obj_copy.data))

    def test_dataio_slice_delegation(self):
        indata = np.arange(30)
        dset = DataIO(indata)
        self.assertTrue(np.all(dset[2:15] == indata[2:15]))

        indata = np.arange(50).reshape(5, 10)
        dset = DataIO(indata)
        self.assertTrue(np.all(dset[1:3, 5:8] == indata[1:3, 5:8]))


if __name__ == '__main__':
    unittest.main()
