from copy import copy, deepcopy

import numpy as np
from hdmf.container import Data
from hdmf.data_utils import DataIO
from hdmf.testing import TestCase


class DataIOTests(TestCase):

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

    def test_set_dataio(self):
        """
        Test that Data.set_dataio works as intended
        """
        dataio = DataIO()
        data = np.arange(30).reshape(5, 2, 3)
        container = Data('wrapped_data', data)
        container.set_dataio(dataio)
        self.assertIs(dataio.data, data)
        self.assertIs(dataio, container.data)

    def test_set_dataio_data_already_set(self):
        """
        Test that Data.set_dataio works as intended
        """
        dataio = DataIO(data=np.arange(30).reshape(5, 2, 3))
        data = np.arange(30).reshape(5, 2, 3)
        container = Data('wrapped_data', data)
        with self.assertRaisesWith(ValueError, "cannot overwrite 'data' on DataIO"):
            container.set_dataio(dataio)
