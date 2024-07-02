from hdmf.data_utils import append_data
from hdmf.testing import TestCase

import numpy as np
from numpy.testing import assert_array_equal
import unittest

try:
    import zarr
    ZARR_INSTALLED = True
except ImportError:
    ZARR_INSTALLED = False

class TestAppendData(TestCase):

    def test_append_data_unknown_error(self):
        obj = object()
        msg = "Data cannot append to object of type '<class 'object'>'"
        with self.assertRaisesWith(ValueError, msg):
            append_data(obj, 4)

    @unittest.skipIf(ZARR_INSTALLED, "optional Zarr package is installed")
    def test_append_data_zarr_error(self):
        zarr_array = zarr.array([1,2,3])
        msg = "Data cannot append to object of type '<class 'zarr.core.Array'>'"
        with self.assertRaisesWith(ValueError, msg):
            append_data(zarr_array, 4)

    @unittest.skipIf(not ZARR_INSTALLED, "optional Zarr package is not installed")
    def test_append_data_zarr(self):
        zarr_array = zarr.array([1,2,3])
        new = append_data(zarr_array, 4)

        assert_array_equal(new[:], np.array([1,2,3,4]))
