import os
import unittest2 as unittest
import numpy as np
from hdmf.container import Data

from hdmf.backends.hdf5 import H5DataIO

class TestDataIO(unittest.TestCase):

    def test_set_dataio(self):
        """
        Test that Data.set_dataio works as intended
        """
        dataio = H5DataIO(chunks=(1, 1, 3))
        data = np.arange(30).reshape(5, 2, 3)
        container = Data('wrapped_data', data)
        container.set_dataio(dataio)
        self.assertIs(dataio.data, data)
        self.assertIs(dataio, container.data)
