import unittest
import numpy as np

from hdmf.build.objectmapper import ObjectMapper
from hdmf.spec import DatasetSpec
from hdmf.testing import TestCase


class TestComplexProtection(TestCase):
    """Test that complex numbers are properly rejected."""

    def setUp(self):
        self.spec = DatasetSpec('an example dataset', 'float64', name='data')

    def test_single_complex_number(self):
        """Test that a single complex number is rejected."""
        with self.assertRaises(ValueError) as cm:
            ObjectMapper.convert_dtype(self.spec, 1 + 2j)
        self.assertEqual(str(cm.exception), "Complex numbers are not supported")

    def test_complex_array(self):
        """Test that an array of complex numbers is rejected."""
        with self.assertRaises(ValueError) as cm:
            ObjectMapper.convert_dtype(self.spec, np.array([1 + 2j, 3 + 4j]))
        self.assertEqual(str(cm.exception), "Complex numbers are not supported")

    def test_complex_in_list(self):
        """Test that a list containing complex numbers is rejected."""
        with self.assertRaises(ValueError) as cm:
            ObjectMapper.convert_dtype(self.spec, [1.0, 2 + 3j, 4.0])
        self.assertEqual(str(cm.exception), "Complex numbers are not supported")

    def test_real_array(self):
        """Test that a real array is not rejected."""
        ret, ret_dtype = ObjectMapper.convert_dtype(self.spec, np.array([1.0, 2.0, 3.0]))
        self.assertIsInstance(ret, np.ndarray)

    def test_real_number(self):
        """Test that a real number is not rejected."""
        ret, ret_dtype = ObjectMapper.convert_dtype(self.spec, 3.14)
        self.assertIsInstance(ret, np.float64)


if __name__ == '__main__':
    unittest.main()
