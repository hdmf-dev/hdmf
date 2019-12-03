import re
import unittest
import h5py
import numpy as np
import os

from ..container import Container, Data
from ..query import HDMFDataset

from ..common import validate as common_validate, get_manager
from ..backends.hdf5 import HDF5IO


class TestCase(unittest.TestCase):
    """
    Extension of unittest's TestCase to add useful functions for unit testing in HDMF.
    """

    def assertRaisesWith(self, exc_type, exc_msg, *args, **kwargs):
        """
        Asserts the given invocation raises the expected exception. This is similar to unittest's assertRaises and
        assertRaisesRegex, but checks for an exact match.
        """

        return self.assertRaisesRegex(exc_type, '^%s$' % re.escape(exc_msg), *args, **kwargs)

    def assertWarnsWith(self, warn_type, exc_msg, *args, **kwargs):
        """
        Asserts the given invocation raises the expected warning. This is similar to unittest's assertWarns and
        assertWarnsRegex, but checks for an exact match.
        """

        return self.assertWarnsRegex(warn_type, '^%s$' % re.escape(exc_msg), *args, **kwargs)

    def assertContainerEqual(self, container1, container2):
        type1 = type(container1)
        type2 = type(container2)
        self.assertEqual(type1, type2)
        for field in getattr(container1, type1._fieldsname):
            with self.subTest(nwbfield=field, container_type=type1.__name__):
                f1 = getattr(container1, field)
                f2 = getattr(container2, field)
                self._assert_field_equal(f1, f2)

    def _assert_field_equal(self, f1, f2):
        if (isinstance(f1, (tuple, list, np.ndarray, h5py.Dataset))
                or isinstance(f2, (tuple, list, np.ndarray, h5py.Dataset))):
            self._assert_array_equal(f1, f2)
        elif isinstance(f1, dict) and len(f1) and isinstance(f1.values()[0], Container):
            self.assertIsInstance(f2, dict)
            f1_keys = set(f1.keys())
            f2_keys = set(f2.keys())
            self.assertSetEqual(f1_keys, f2_keys)
            for k in f1_keys:
                with self.subTest(module_name=k):
                    self.assertContainerEqual(f1[k], f2[k])
        elif isinstance(f1, Container):
            self.assertContainerEqual(f1, f2)
        elif isinstance(f1, Data):
            self._assert_data_equal(f1, f2)
        elif isinstance(f1, (float, np.floating)):
            np.testing.assert_equal(f1, f2)
        else:
            self.assertEqual(f1, f2)

    def _assert_data_equal(self, data1, data2):
        self.assertEqual(type(data1), type(data2))
        self.assertEqual(len(data1), len(data2))
        self._assert_array_equal(data1.data, data2.data)

    def _assert_array_equal(self, arr1, arr2):
        if isinstance(arr1, (h5py.Dataset, HDMFDataset)):
            arr1 = arr1[()]
        if isinstance(arr2, (h5py.Dataset, HDMFDataset)):
            arr2 = arr2[()]
        if not isinstance(arr1, (tuple, list, np.ndarray)) and not isinstance(arr2, (tuple, list, np.ndarray)):
            if isinstance(arr1, (float, np.floating)):
                np.testing.assert_equal(arr1, arr2)
            else:
                self.assertEqual(arr1, arr2)  # scalar
        else:
            self.assertEqual(len(arr1), len(arr2))
            if isinstance(arr1, np.ndarray) and len(arr1.dtype) > 1:  # compound type
                arr1 = arr1.tolist()
            if isinstance(arr2, np.ndarray) and len(arr2.dtype) > 1:  # compound type
                arr2 = arr2.tolist()
            if isinstance(arr1, np.ndarray) and isinstance(arr2, np.ndarray):
                np.testing.assert_array_equal(arr1, arr2)
            else:
                for sub1, sub2 in zip(arr1, arr2):
                    if isinstance(sub1, Container):
                        self.assertContainerEqual(sub1, sub2)
                    elif isinstance(sub1, Data):
                        self._assert_data_equal(sub1, sub2)
                    else:
                        self._assert_array_equal(sub1, sub2)


class TestMapH5RoundTrip(TestCase):

    def setUpContainer(self):
        ''' Should return the Container to build and read/write'''
        raise unittest.SkipTest('Cannot run test unless setUpContainer is implemented')

    def setUp(self):
        self.container = self.setUpContainer()
        self.object_id = self.container.object_id
        self.container_type = self.container.__class__.__name__
        self.filename = 'test_%s.h5' % self.container_type
        self.writer = None
        self.reader = None

    def tearDown(self):
        if self.writer is not None:
            self.writer.close()
        if self.reader is not None:
            self.reader.close()
        if os.path.exists(self.filename) and os.getenv("CLEAN_HDMF", '1') not in ('0', 'false', 'FALSE', 'False'):
            os.remove(self.filename)

    def roundtripContainer(self, cache_spec=False):
        self.writer = HDF5IO(self.filename, manager=get_manager(), mode='w')
        self.writer.write(self.container, cache_spec=cache_spec)
        self.writer.close()
        self.reader = HDF5IO(self.filename, manager=get_manager(), mode='r')
        try:
            return self.reader.read()
        except Exception as e:
            self.reader.close()
            self.reader = None
            raise e

    def test_roundtrip(self):
        self.read_container = self.roundtripContainer()
        # make sure we get a completely new object
        self.assertIsNotNone(str(self.container))  # added as a test to make sure printing works
        self.assertIsNotNone(str(self.read_container))
        self.assertNotEqual(id(self.container), id(self.read_container))
        self.assertContainerEqual(self.read_container, self.container)
        self.reader.close()
        self.validate()

    def validate(self):
        # validate created file
        if os.path.exists(self.filename):
            with HDF5IO(self.filename, manager=get_manager(), mode='r') as io:
                errors = common_validate(io)
                if errors:
                    for err in errors:
                        raise Exception(err)
