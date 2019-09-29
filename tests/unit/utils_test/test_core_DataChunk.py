import unittest2 as unittest

from hdmf.data_utils import DataChunk
import numpy as np
from copy import copy, deepcopy


class DataChunkTests(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_datachunk_copy(self):
        obj = DataChunk(data=np.arange(3), selection=np.s_[0:3])
        obj_copy = copy(obj)
        self.assertNotEqual(id(obj), id(obj_copy))
        self.assertEqual(id(obj.data), id(obj_copy.data))
        self.assertEqual(id(obj.selection), id(obj_copy.selection))

    def test_datachunk_deepcopy(self):
        obj = DataChunk(data=np.arange(3), selection=np.s_[0:3])
        obj_copy = deepcopy(obj)
        self.assertNotEqual(id(obj), id(obj_copy))
        self.assertNotEqual(id(obj.data), id(obj_copy.data))
        self.assertNotEqual(id(obj.selection), id(obj_copy.selection))

    def test_datachunk_astype(self):
        obj = DataChunk(data=np.arange(3), selection=np.s_[0:3])
        newtype = np.dtype('int16')
        obj_astype = obj.astype(newtype)
        self.assertNotEqual(id(obj), id(obj_astype))
        self.assertEqual(obj_astype.dtype, np.dtype(newtype))


if __name__ == '__main__':
    unittest.main()
