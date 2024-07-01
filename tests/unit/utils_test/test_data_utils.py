from hdmf.data_utils import append_data
from hdmf.testing import TestCase

import numpy as np
from numpy.testing import assert_array_equal

try:
    import zarr
    ZARR_INSTALLED = True
except ImportError:
    ZARR_INSTALLED = False

class TestAppendData(TestCase):

    def setUp(self):
        if not ZARR_INSTALLED:
            self.skipTest("optional Zarr package is not installed")

    def test_append_data_zarr(self):
        zarr_array = zarr.array([1,2,3])
        new = append_data(zarr_array, 4)

        assert_array_equal(new[:], np.array([1,2,3,4]))
